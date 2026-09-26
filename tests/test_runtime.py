"""Run the real lifecycle against deterministic exchange transport doubles."""
import asyncio
import json
import uuid
from collections import deque
from types import SimpleNamespace

from entropy_robinhood_lighter_arbitrage.config import HLCreds, LighterCreds
from entropy_robinhood_lighter_arbitrage.engine import Engine
from entropy_robinhood_lighter_arbitrage.journal import OrderJournal
from tests.test_engine import book_with, make_cfg


def runtime_config(tmp_path, **overrides):
    values = {
        "state_db": str(tmp_path / "orders.sqlite3"), "health_file": str(tmp_path / "health.json"),
        "trades_csv": str(tmp_path / "trades.csv"),
        "submit_timeout_sec": 0.03, "settle_timeout_sec": 0.03, "recovery_poll_sec": 0.005,
        "reconcile_grace_sec": 0, "recovery_timeout_sec": 2, "reconcile_sec": 0.01,
        "shutdown_timeout_sec": 0.08, "min_disk_free_mb": 0,
        "leg_slippage_bps": 1, "premium_persist_sec": 0.005}
    values.update(overrides)
    cfg = make_cfg(**values)
    cfg.entropy.hl_creds = HLCreds("offline-placeholder", "offline-account")
    cfg.hedge.lighter_creds = LighterCreds(1, 1, "offline-placeholder")
    return cfg


def fill(qty, px=100, unresolved=False):
    return {"status": "timeout" if unresolved else "filled", "filled_base": qty,
            "avg_px": px if qty else None, "err": None, "unresolved": unresolved}


class Exchange:
    def __init__(self, conf, bids, asks):
        self.conf = conf
        self.key, self.name, self.kind = conf.key, conf.label, conf.kind
        self.cap_usd, self.fee_bps, self.orders_per_min = conf.cap_usd, 0, 120
        self.book = book_with(bids, asks)
        self.bids, self.asks = bids, asks
        self.api_url, self.coin = "offline", "io:SNDK"
        self.profile, self.market_id = SimpleNamespace(api_url="offline"), 1
        self.size_decimals, self.min_base, self.min_quote = 4, 0.0001, 10
        self.position = self.remote_position = self.cash = self.volume_usd = 0.0
        self.equity = self.free = self.start_equity = None
        self.last_traded_ts = 0
        self.sends, self.outcomes, self.resolutions = [], deque(), {}
        self.closed = False
        self.stale_position = None
        self.block_send = None

    async def load_market(self):
        pass

    def init_signer(self):
        pass

    def _query_address(self):
        return "offline-account"

    def new_client_order_id(self):
        return uuid.uuid4().hex

    def ready_to_trade(self):
        return True

    def px_round(self, px, round_up):
        return px

    def start_tasks(self, stop, notify, live):
        async def feed():
            while not stop.is_set():
                self.book.apply_hl([[{"px": p, "sz": q} for p, q in self.bids],
                                    [{"px": p, "sz": q} for p, q in self.asks]])
                notify()
                await asyncio.sleep(0.002)
        return [asyncio.create_task(feed(), name=f"fake-{self.key}")]

    async def send_taker(self, **order):
        self.sends.append(order)
        if self.block_send:
            await self.block_send.wait()
        result = self.outcomes.popleft() if self.outcomes else fill(order["qty"], order["limit_px"])
        if not result["unresolved"]:
            self.remote_position += result["filled_base"] * (1 if order["is_buy"] else -1)
        return result

    async def resolve_order(self, oid):
        return self.resolutions.get(oid, fill(0, unresolved=True))

    async def fetch_position(self):
        return self.remote_position if self.stale_position is None else self.stale_position

    async def fetch_equity(self):
        return 1000.0, 1000.0

    async def warm_http(self):
        pass

    async def close(self):
        self.closed = True


class OfflineEngine(Engine):
    def __init__(self, cfg, venues):
        super().__init__(cfg)
        self.exchanges = venues

    def _make_venue(self, conf):
        return self.exchanges[conf.key]


def exchanges(cfg):
    return {"entropy": Exchange(cfg.entropy, [(99.9, 10)], [(100, 10)]),
            "hedge": Exchange(cfg.hedge, [(100.2, 10)], [(100.3, 10)])}


async def until(predicate, timeout=1):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.002)
    await asyncio.wait_for(poll(), timeout)


def test_unknown_blocks_new_orders_and_restart_recovers_once(tmp_path):
    async def scenario():
        cfg = runtime_config(tmp_path)
        vs = exchanges(cfg)
        vs["hedge"].outcomes.append(fill(0, unresolved=True))
        eng = OfflineEngine(cfg, vs)
        run = asyncio.create_task(eng.run())
        try:
            await until(lambda: len(vs["hedge"].sends) == 1 and bool(eng._pending()))
            await asyncio.sleep(0.04)
            assert len(vs["entropy"].sends) == len(vs["hedge"].sends) == 1
            assert eng.recovering
        finally:
            eng.request_stop()
            await run
        oid = vs["hedge"].sends[0]["client_order_id"]
        qty = vs["hedge"].sends[0]["qty"]
        # The first process crashed before learning this fill. Its restart gets
        # exactly one terminal lookup and verifies the actual pair positions.
        vs["hedge"].remote_position = -qty
        vs["hedge"].resolutions[oid] = fill(qty, 100.2)
        vs["hedge"].bids = [(99.8, 10)]  # no new signal during recovery assertion
        second = OfflineEngine(cfg, vs)
        run2 = asyncio.create_task(second.run())
        try:
            await until(lambda: second._positions_trusted and not second.recovering)
            assert second._pending() == []
            assert abs(sum(v.position for v in second.venues.values())) < 1e-9
            assert len(vs["hedge"].sends) == 1
            expected = second.journal.get_meta("positions")
            assert expected["hedge"] == -qty
        finally:
            second.request_stop()
            await run2
    asyncio.run(scenario())


def test_partial_hedges_are_retried_without_new_arbitrage(tmp_path):
    async def scenario():
        cfg = runtime_config(tmp_path)
        vs = exchanges(cfg)
        vs["entropy"].remote_position = 3
        vs["hedge"].remote_position = -1
        vs["entropy"].outcomes.extend([fill(1), fill(1)])
        vs["hedge"].bids = [(99.8, 10)]
        eng = OfflineEngine(cfg, vs)
        run = asyncio.create_task(eng.run())
        try:
            await until(lambda: len(vs["entropy"].sends) >= 2 and not eng.recovering)
            assert all(order["reduce_only"] for order in vs["entropy"].sends)
            assert len(vs["hedge"].sends) == 0
            assert abs(sum(v.position for v in eng.venues.values())) < 1e-9
        finally:
            eng.request_stop()
            await run
    asyncio.run(scenario())


def test_shutdown_retains_submitted_order_and_drains_all_tasks(tmp_path):
    async def scenario():
        cfg = runtime_config(tmp_path)
        vs = exchanges(cfg)
        vs["hedge"].block_send = asyncio.Event()
        eng = OfflineEngine(cfg, vs)
        run = asyncio.create_task(eng.run())
        await until(lambda: bool(vs["hedge"].sends))
        eng.request_stop()
        await asyncio.wait_for(run, timeout=1)
        assert all(v.closed for v in vs.values())
        assert not eng._order_tasks and not eng._exec_tasks
        health = json.loads((tmp_path / "health.json").read_text())
        assert health["state"] == "STOPPED" and health["pending_orders"] == 1
    asyncio.run(scenario())


def test_stale_position_cannot_undo_confirmed_fill(tmp_path):
    async def scenario():
        cfg = runtime_config(tmp_path)
        vs = exchanges(cfg)
        eng = OfflineEngine(cfg, vs)
        run = asyncio.create_task(eng.run())
        try:
            await until(lambda: eng.trades >= 1)
            before = eng.journal.get_meta("positions")
            for v in vs.values():
                v.stale_position = 0
            vs["hedge"].bids = [(99.8, 10)]
            await until(lambda: not eng._positions_trusted)
            assert eng.journal.get_meta("positions") == before
            assert eng.recovering
            assert not any(o["reduce_only"] for v in vs.values() for o in v.sends)
        finally:
            eng.request_stop()
            await run
    asyncio.run(scenario())


def test_slippage_and_depth_caps_cannot_approve_loss_or_excess(tmp_path):
    cfg = runtime_config(tmp_path)
    vs = exchanges(cfg)
    eng = OfflineEngine(cfg, vs)
    eng.entropy, eng.hedge = vs.values()
    # Reserve 50 bps per leg; a 20 bps observed signal is insufficient.
    cfg.leg_slippage_bps = 50
    assert eng._plan(vs["entropy"], vs["hedge"], 500)[0] is None
    cfg.leg_slippage_bps = 0
    cfg.max_order_notional = 100
    for v in vs.values():
        v.cap_usd = 100
    vs["entropy"].book = book_with([(99, 100)], [(100, 0.5), (110, 100)])
    vs["hedge"].book = book_with([(120, 100)], [(121, 100)])
    plan, _ = eng._plan(vs["entropy"], vs["hedge"], 100)
    if plan:
        assert max(plan.buy_notional, plan.sell_notional) <= 100
        assert plan.qty * 121 <= 100 + 1e-8


def test_journal_expected_position_is_atomic_and_halt_survives_restart(tmp_path):
    path = str(tmp_path / "orders.sqlite3")
    j = OrderJournal(path, "pair")
    j.set_meta("positions", {"entropy": 0.0, "hedge": 0.0})
    intent = {"id": "one", "venue": "entropy", "is_buy": True,
              "qty": 1, "limit_px": 100, "reduce_only": False, "batch_id": "b"}
    j.prepare_batch([intent])
    j.mark_submitted("one")
    j.record_result("one", fill(1))
    j.set_meta("halt_reason", "drawdown")
    j.close()
    reopened = OrderJournal(path, "pair")
    assert reopened.get_meta("positions")["entropy"] == 1
    assert not reopened.record_result("one", fill(1))
    assert reopened.get_meta("positions")["entropy"] == 1
    assert reopened.get_meta("halt_reason") == "drawdown"
    reopened.close()
