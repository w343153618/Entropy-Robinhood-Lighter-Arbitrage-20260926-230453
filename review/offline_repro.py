"""Offline evidence for the review of b0f02e9.

Run with the base requirements installed: python review/offline_repro.py
No HTTP/WS connections, credentials, signing SDKs, or real orders are used.
Historical evidence for baseline b0f02e9, before the hardening changes.
These assertions intentionally describe the OLD defects and are NOT regression
tests for the repaired code. Current acceptance tests live under tests/.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import runpy
import sys
import tempfile
import time
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from entropy_robinhood_lighter_arbitrage.book import ArbPlan, OrderBook
from entropy_robinhood_lighter_arbitrage.config import (
    LIGHTER_PROFILES,
    HLCreds,
    LighterCreds,
    VenueConf,
    load_config,
)
from entropy_robinhood_lighter_arbitrage.engine import Engine
from entropy_robinhood_lighter_arbitrage.feeds import HLBookFeed, LighterBookFeed
from entropy_robinhood_lighter_arbitrage.recorder import (
    CSV_HEADER,
    MinuteRecorder,
)
from entropy_robinhood_lighter_arbitrage.venue_lighter import (
    AccountOrdersFeed,
    LighterVenue,
)

HELPERS = runpy.run_path(str(ROOT / "tests/test_engine.py"))
make_cfg = HELPERS["make_cfg"]
book_with = HELPERS["book_with"]
FakeVenue = HELPERS["FakeVenue"]


def emit(case, **evidence):
    print(json.dumps({"case": case, **evidence}, ensure_ascii=False))


def venue(key, bids, asks, cap=1000):
    v = FakeVenue(key, key, book_with(bids, asks), cap, 0)
    v.cash = 0
    v.volume_usd = 0
    v.min_base = 0.0001
    v.min_quote = 10
    v.ready_to_trade = lambda: True
    v.px_round = lambda px, round_up: px
    v.fetch_position = AsyncMock(return_value=0)
    return v


def engine(buy, sell, **config):
    eng = Engine(make_cfg(**config))
    eng.entropy, eng.hedge = buy, sell
    eng.venues = {buy.key: buy, sell.key: sell}
    eng._log_csv = lambda *args: None
    return eng


async def unresolved_barrier():
    buy = venue("entropy", [(99.9, 10)], [(100, 10)])
    sell = venue("hedge", [(100.1, 10)], [(100.2, 10)])
    eng = engine(buy, sell)
    for v in (buy, sell):
        v.send_taker = AsyncMock(return_value={
            "status": "timeout", "filled_base": 0,
            "avg_px": None, "err": None, "unresolved": True,
        })
        await eng._vlock(v.key).acquire()
    plan, _ = eng._plan(buy, sell, 500)
    assert plan is not None
    await eng._execute_locked(buy, sell, plan)
    await eng._reconcile_positions(hedge=True)
    fetches = [v.fetch_position.await_count for v in (buy, sell)]
    for v in (buy, sell):
        v.book.last_update_ts = v.last_traded_ts + 0.001
    eng._scan(time.time())  # arm the direction
    can_open_again = eng._scan(time.time()) is not None
    assert fetches == [0, 0] and can_open_again and not eng.halted
    emit("unknown_outcome_has_no_barrier", position_reads=fetches,
         can_open_again=can_open_again, halted=eng.halted)


async def nonce_gap():
    book = OrderBook()
    ws = SimpleNamespace(send=AsyncMock())
    feed = LighterBookFeed("RH", "unused", 32, book, lambda: None)
    await feed._handle_book(ws, {
        "channel": "order_book/32", "order_book": {
            "nonce": 10, "bids": [{"price": "100", "size": "5"}],
            "asks": [{"price": "101", "size": "5"}],
        }}, snapshot=True)
    # The missing transition 10 -> 11 deleted bid 100.
    await feed._handle_book(ws, {
        "channel": "order_book/32", "order_book": {
            "begin_nonce": 11, "nonce": 12,
            "bids": [{"price": "99", "size": "5"}], "asks": [],
        }}, snapshot=False)
    assert book.best_bid() == 100 and feed._synced and ws.send.await_count == 0
    emit("one_nonce_gap_keeps_ghost_bid", best_bid=book.best_bid(),
         fresh=book.is_fresh(10), resubscribe_messages=ws.send.await_count)


async def accepted_then_timeout():
    conf = VenueConf("hedge", "lighter", "RH", "SNDK", 0, 1000, 30,
                     lighter_profile=LIGHTER_PROFILES["lighter-rh"])
    v = LighterVenue(conf, None, 0.01)
    v.market_id = 32
    v.orders_feed = AccountOrdersFeed("RH", "unused", 32, 1, None)
    accepted = []

    async def fake_create_order(**kwargs):
        accepted.append(kwargs["client_order_index"])
        raise TimeoutError("response lost after acceptance")

    v.signer = SimpleNamespace(create_order=fake_create_order)
    constants = SimpleNamespace(
        ORDER_TYPE_MARKET=1, ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0,
        DEFAULT_IOC_EXPIRY=0,
    )
    with patch.dict(sys.modules, {"lighter": SimpleNamespace(SignerClient=constants)}):
        result = await v.send_taker(is_buy=False, qty=1, limit_px=100)
    coi = accepted[0]
    v.orders_feed._handle_orders({"orders": {"32": [{
        "client_order_index": coi, "status": "filled",
        "filled_base_amount": "1", "filled_quote_amount": "100",
    }]}})
    assert result["unresolved"] is False and result["filled_base"] == 0
    assert not v.orders_feed._pending
    emit("accepted_order_timeout_misclassified", returned=result,
         late_fill=v.orders_feed._terminal[coi]["filled_base"],
         pending_watchers=len(v.orders_feed._pending))


async def slippage_budget():
    buy = venue("entropy", [(99.99, 10)], [(100, 10)])
    sell = venue("hedge", [(100.04, 10)], [(100.05, 10)])
    eng = engine(buy, sell)

    async def fill_at_bound(**kwargs):
        return {"status": "filled", "filled_base": kwargs["qty"],
                "avg_px": kwargs["limit_px"], "err": None, "unresolved": False}

    buy.send_taker = sell.send_taker = fill_at_bound
    plan = ArbPlan(1, 100, 100.04, 100, 100.04, 1, 100, 4, 4, 0, 0)
    await eng._execute(buy, sell, plan)
    assert eng.total_fill_edge < 0
    emit("allowed_slippage_exceeds_signal", planned_edge_usd=plan.exp_edge_usd,
         actual_edge_usd=eng.total_fill_edge,
         per_leg_slippage_bps=eng.cfg.leg_slippage_bps)


async def send_deadline():
    conf = VenueConf("hedge", "lighter", "RH", "SNDK", 0, 1000, 30,
                     lighter_profile=LIGHTER_PROFILES["lighter-rh"])
    v = LighterVenue(conf, None, 0.02)
    v.orders_feed = AccountOrdersFeed("RH", "unused", 32, 1, None)
    never_reply = asyncio.Event()
    v.signer = SimpleNamespace(create_order=AsyncMock(side_effect=never_reply.wait))

    async def hanging_create_order(**kwargs):
        await never_reply.wait()

    v.signer.create_order = hanging_create_order
    constants = SimpleNamespace(
        ORDER_TYPE_MARKET=1, ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0,
        DEFAULT_IOC_EXPIRY=0,
    )
    with patch.dict(sys.modules, {"lighter": SimpleNamespace(SignerClient=constants)}):
        task = asyncio.create_task(v.send_taker(is_buy=False, qty=1, limit_px=100))
        await asyncio.sleep(0.05)
        pending = not task.done()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert pending
    emit("submission_not_bounded_by_settle_timeout", settle_timeout_sec=0.02,
         still_pending_after_sec=0.05)


async def partial_hedge():
    buy = venue("entropy", [(99.9, 10)], [(100, 10)])
    sell = venue("hedge", [(100.1, 10)], [(100.2, 10)])
    buy.position, sell.position = 3, -1
    buy.send_taker = AsyncMock(return_value={
        "status": "canceled", "filled_base": 1,
        "avg_px": 99.9, "err": None, "unresolved": False,
    })
    eng = engine(buy, sell)
    await eng._maybe_hedge()
    net = buy.position + sell.position
    for v in (buy, sell):
        v.book.last_update_ts = max(time.time(), v.last_traded_ts + 0.001)
    eng._scan(time.time())
    allowed = eng._scan(time.time()) is not None
    assert net == 1 and not eng._reconcile_evt.is_set() and allowed
    emit("partial_hedge_leaves_residual", residual_net_base=net,
         tolerance_base=eng.cfg.net_tolerance_base,
         hedge_calls=buy.send_taker.await_count, can_open_again=allowed)


async def shutdown_pending():
    buy = venue("entropy", [(99.9, 10)], [(100, 10)])
    sell = venue("hedge", [(100.1, 10)], [(100.2, 10)])
    eng = engine(buy, sell, settle_timeout_sec=0.01)
    eng.cfg.entropy.hl_creds = HLCreds("test-placeholder", "test-account")
    eng.cfg.hedge.lighter_creds = LighterCreds(1, 1, "test-placeholder")
    started, release = asyncio.Event(), asyncio.Event()

    async def pending_send(**kwargs):
        started.set()
        await release.wait()
        return {"status": "canceled", "filled_base": 0,
                "avg_px": None, "err": None, "unresolved": False}

    for v, kind in ((buy, "hl"), (sell, "lighter")):
        v.kind = kind
        v.conf = eng.cfg.entropy if v.key == "entropy" else eng.cfg.hedge
        v.size_decimals = 4
        v.load_market = AsyncMock()
        v.init_signer = lambda: None
        v.start_tasks = lambda stop, notify, live: (notify(), [])[1]
        v.fetch_equity = AsyncMock(return_value=None)
        v.close = AsyncMock()
        v.send_taker = pending_send
    eng._make_venue = lambda conf: buy if conf.key == "entropy" else sell
    task = asyncio.create_task(eng._run_inner())
    await asyncio.wait_for(started.wait(), timeout=1)
    eng.request_stop()
    await asyncio.wait_for(task, timeout=3)
    pending_count = sum(not t.done() for t in eng._exec_tasks)
    closed = [v.close.await_count for v in (buy, sell)]
    assert pending_count == 1 and closed == [1, 1]
    # Complete the fake operation so the evidence script leaves no idle task.
    remaining = list(eng._exec_tasks)
    release.set()
    await asyncio.gather(*remaining)
    emit("shutdown_closes_with_pending_order", pending_executions_at_close=pending_count,
         adapter_close_calls=closed, shutdown_budget_sec=2.01)


async def depth_caps():
    buy = venue("entropy", [(99, 100)], [(100, 0.5), (110, 100)], cap=100)
    sell = venue("hedge", [(120, 100)], [(121, 100)], cap=100)
    eng = engine(buy, sell, max_order_notional=100, take_fraction=1)
    eng._scan(time.time())
    best = eng._scan(time.time())
    assert best is not None
    plan = best[2]
    assert plan.buy_notional > 100 and plan.sell_notional > 100
    emit("depth_walk_exceeds_caps", configured_order_cap_usd=100,
         each_venue_cap_usd=100, qty=plan.qty,
         buy_notional_usd=plan.buy_notional, sell_notional_usd=plan.sell_notional)


def stale_hl_book():
    book = OrderBook()
    feed = HLBookFeed("HL", "unused", "io:SNDK", book, lambda: None)
    with patch("time.time", return_value=100):
        feed._on_frame({"channel": "l2Book", "data": {
            "coin": "io:SNDK", "levels": [[{"px": "100", "sz": "1"}],
                                           [{"px": "101", "sz": "1"}]],
        }})
    with patch("time.time", return_value=1000):
        feed._on_frame({"channel": "pong"})
        fresh = book.is_fresh(10)
    assert fresh
    emit("pong_keeps_old_hl_snapshot_fresh", snapshot_age_sec=900,
         freshness_limit_sec=10, reported_fresh=fresh)


async def recorder_cancel():
    with tempfile.TemporaryDirectory(prefix="entropy-stop-") as task_dir:
        path = Path(task_dir) / "minutes.csv"
        book = book_with([(100, 1)], [(101, 1)])
        rec = MinuteRecorder(str(path), book, book, 10)
        stop = asyncio.Event()
        task = asyncio.create_task(rec.run(stop))
        await asyncio.sleep(0)
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert len(rec._samples) == 1 and not path.exists()
        emit("recorder_cancel_loses_tail", unwritten_samples=len(rec._samples),
             csv_exists=path.exists())


async def invalid_config():
    with tempfile.TemporaryDirectory(prefix="entropy-config-") as task_dir:
        cfg_path = Path(task_dir) / "config.yaml"
        cfg_path.write_text("thresholds:\n  midline_bps: 0\n  upper_bps: 4\n"
                            "  lower_bps: 4\nexecution:\n  net_tolerance_base: .nan\n")
        with patch("entropy_robinhood_lighter_arbitrage.config.load_dotenv"), \
                patch("entropy_robinhood_lighter_arbitrage.config._env_s", return_value=None), \
                patch("entropy_robinhood_lighter_arbitrage.config._env_i", return_value=None):
            cfg = load_config(str(cfg_path), symbol="SNDK", hedge_venue="lighter-rh")
    eng = Engine(cfg)
    eng.venues = {"entropy": SimpleNamespace(position=1),
                  "hedge": SimpleNamespace(position=0)}
    eng._hedge = AsyncMock()
    await eng._maybe_hedge()
    assert eng._hedge.await_count == 0
    emit("nan_config_disables_hedging", net_base=1,
         nan_accepted=cfg.net_tolerance_base != cfg.net_tolerance_base,
         hedge_calls=eng._hedge.await_count)


def mixed_markets_and_entrypoint():
    analyzer = runpy.run_path(str(ROOT / "tools/analyze.py"))
    with tempfile.TemporaryDirectory(prefix="entropy-record-") as task_dir:
        path = Path(task_dir) / "minutes.csv"
        for prem in (0, 1000):
            recorder = MinuteRecorder(str(path), OrderBook(), OrderBook(), 10)
            recorder._minute_start = int(time.time() // 60) * 60
            sample = {"prem": prem, "sell_edge": prem, "buy_edge": -prem,
                      "eb": 100 + prem / 100, "ea": 100 + prem / 100,
                      "hb": 100, "ha": 100}
            recorder._samples = [sample] * 60
            recorder._flush()
        rows = analyzer["load_rows"](str(path), 0, 10)
    median = analyzer["pctl"](sorted(r["prem"] for r in rows), 50)
    assert median == 500 and "symbol" not in CSV_HEADER
    emit("mixed_market_calibration", rows=len(rows), median_premium_bps=median,
         has_symbol="symbol" in CSV_HEADER, has_hedge_venue="hedge_venue" in CSV_HEADER)
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    entrypoint = config["project"]["scripts"]["entropy-rh-arb"]
    module = entrypoint.split(":", 1)[0]
    assert importlib.util.find_spec(module) is None
    emit("console_entrypoint_missing", entrypoint=entrypoint, module_exists=False)


async def main():
    await unresolved_barrier()
    await nonce_gap()
    await accepted_then_timeout()
    await slippage_budget()
    await send_deadline()
    await partial_hedge()
    await shutdown_pending()
    await depth_caps()
    stale_hl_book()
    await recorder_cancel()
    await invalid_config()
    mixed_markets_and_entrypoint()


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)  # Fake engine lifecycle also logs LIVE.
    asyncio.run(main())
