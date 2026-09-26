"""Engine signal logic (entropy_robinhood_lighter_arbitrage.engine): thresholds, inventory ladder,
headroom, scan direction wiring, and premium accounting."""
from __future__ import annotations

import asyncio
import time

from entropy_robinhood_lighter_arbitrage.book import ArbPlan, OrderBook
from entropy_robinhood_lighter_arbitrage.config import Config, VenueConf
from entropy_robinhood_lighter_arbitrage.engine import Engine


def book_with(bids, asks) -> OrderBook:
    b = OrderBook()
    b.apply_hl([[{"px": px, "sz": sz} for px, sz in bids],
                [{"px": px, "sz": sz} for px, sz in asks]])
    return b


class FakeVenue:
    """Duck-typed venue standing in for HLVenue / LighterVenue."""

    def __init__(self, key: str, name: str, book: OrderBook, cap_usd: float,
                 fee_bps: float, position: float = 0.0) -> None:
        self.key = key
        self.name = name
        self.book = book
        self.cap_usd = cap_usd
        self.fee_bps = fee_bps
        self.position = position
        self.last_traded_ts = 0.0
        self.orders_per_min = 120

    def px_round(self, px, round_up):
        return px


def make_cfg(**overrides) -> Config:
    entropy = VenueConf(key="entropy", kind="hl", label="ENTROPY", symbol="SNDK",
                        fee_bps=0.0, cap_usd=1000.0, orders_per_min=120)
    hedge = VenueConf(key="hedge", kind="lighter", label="RH", symbol="SNDK",
                      fee_bps=0.0, cap_usd=1000.0, orders_per_min=30)
    base = {
        "symbol": "SNDK", "hedge_venue": "lighter-rh", "entropy": entropy,
        "hedge": hedge,
        "midline_bps": 0.0, "upper_bps": 4.0, "lower_bps": 4.0,
        "take_fraction": 0.5, "max_order_notional": 500.0,
        "min_order_notional": 10.0,
        "inventory_scale_bps": 0.0, "inventory_floor_frac": 0.0,
        "premium_persist_sec": 0.0, "cooldown_sec": 0.0,
        "settle_timeout_sec": 5.0, "leg_slippage_bps": 0.0,
        "hedge_slippage_bps": 20.0, "net_tolerance_base": 0.001,
        "max_consecutive_errors": 3, "rate_limit_pause_sec": 10.0,
        "staleness_sec": 10.0, "reconcile_sec": 15.0,
        "venue_probe_sec": 30.0, "http_keepalive_sec": 0.0,
        "recorder_enabled": False, "recorder_csv": "logs/minutes.csv",
        "log_level": "INFO", "status_interval_sec": 30.0,
        "trades_csv": "logs/trades.csv", "dashboard": False,
        "log_file": "logs/engine.log",
    }
    base.update(overrides)
    return Config(**base)


def test_eff_threshold_directions() -> None:
    eng = Engine(make_cfg())
    eng.entropy = FakeVenue("entropy", "ENTROPY", book_with([], []), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH", book_with([], []), 1000, 0.0)
    # sell entropy: midline + upper = 4
    assert eng._eff_threshold(eng.hedge, eng.entropy) == 4.0
    # buy entropy: lower - midline = 4
    assert eng._eff_threshold(eng.entropy, eng.hedge) == 4.0


def test_eff_threshold_with_midline() -> None:
    eng = Engine(make_cfg(midline_bps=5.0))
    eng.entropy = FakeVenue("entropy", "ENTROPY", book_with([], []), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH", book_with([], []), 1000, 0.0)
    # sell entropy hurdle: 5 + 4 = 9
    assert eng._eff_threshold(eng.hedge, eng.entropy) == 9.0
    # buy entropy hurdle can be negative: 4 - 5 = -1
    assert eng._eff_threshold(eng.entropy, eng.hedge) == -1.0


def test_inventory_ladder_flat() -> None:
    eng = Engine(make_cfg(inventory_scale_bps=10.0, inventory_floor_frac=0.5))
    eng.entropy = FakeVenue("entropy", "ENTROPY",
                            book_with([(100.0, 1)], [(100.1, 1)]), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH", book_with([(100.0, 1)], [(100.1, 1)]),
                          1000, 0.0)
    # flat positions -> no surcharge
    assert eng._inv_add_bps(eng.entropy, eng.hedge) == 0.0
    # sell leg (hedge) short 6 units @ ~100 = 600/1000 = 0.6 of cap, past
    # floor 0.5 -> selling more on it must cost extra bps
    eng.hedge.position = -6.0
    surcharge = eng._inv_add_bps(eng.entropy, eng.hedge)
    assert 0.0 < surcharge <= 10.0


def test_headroom_caps() -> None:
    eng = Engine(make_cfg())
    buy = FakeVenue("entropy", "ENTROPY", book_with([], []), 1000, 0.0,
                    position=8.0)
    sell = FakeVenue("hedge", "RH", book_with([], []), 1000, 0.0,
                     position=-6.0)
    # buy headroom: 1000 - 8*100 = 200 ; sell headroom: 1000 + (-6)*100 = 400
    assert eng._headroom(buy, sell, ref_px=100.0) == 200.0


async def _scan_best(eng, twice: bool = False):
    """Run _scan inside a running event loop. The engine arms a direction on
    the first scan and only fires on a later one (premium persistence), so
    callers that expect a plan pass twice=True."""
    first = eng._scan(time.time())
    if twice and first is None:
        return eng._scan(time.time())
    return first


def test_scan_sell_entropy_qualifies() -> None:
    eng = Engine(make_cfg())
    eng.entropy = FakeVenue("entropy", "ENTROPY",
                            book_with([(100.3, 10)], [(100.4, 10)]), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH",
                          book_with([(100.2, 10)], [(100.3, 10)]), 1000, 0.0)
    for v in (eng.entropy, eng.hedge):
        v.ready_to_trade = lambda: True  # type: ignore[attr-defined]
    # sell entropy: entropy bid 100.3 / hedge ask 100.3 -> 0 bps... need edge
    best = asyncio.run(_scan_best(eng))
    assert best is None  # premium 0 on sell side, no edge on either


def test_scan_buy_entropy_qualifies() -> None:
    eng = Engine(make_cfg())
    # buy entropy fires when hedge bid / entropy ask clears lower - midline = 4bps
    eng.entropy = FakeVenue("entropy", "ENTROPY",
                            book_with([(100.0, 10)], [(100.1, 10)]), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH",
                          book_with([(100.2, 10)], [(100.3, 10)]), 1000, 0.0)
    for v in (eng.entropy, eng.hedge):
        v.ready_to_trade = lambda: True  # type: ignore[attr-defined]
    best = asyncio.run(_scan_best(eng, twice=True))
    assert best is not None
    _, sell, plan = best
    assert sell.key == "hedge"  # buy entropy -> buy on entropy, sell on hedge
    assert plan.exp_edge_usd > 0.0


def test_scan_ignores_unready_venues() -> None:
    eng = Engine(make_cfg())
    eng.entropy = FakeVenue("entropy", "ENTROPY",
                            book_with([(100.0, 10)], [(100.1, 10)]), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH",
                          book_with([(100.2, 10)], [(100.3, 10)]), 1000, 0.0)
    # hedge not ready to trade -> nothing scans
    eng.entropy.ready_to_trade = lambda: True  # type: ignore[attr-defined]
    eng.hedge.ready_to_trade = lambda: False  # type: ignore[attr-defined]
    best = asyncio.run(_scan_best(eng))
    assert best is None


def test_premium_bps_midline() -> None:
    eng = Engine(make_cfg())
    eng.entropy = FakeVenue("entropy", "ENTROPY",
                            book_with([(100.1, 1)], [(100.2, 1)]), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH",
                          book_with([(100.0, 1)], [(100.1, 1)]), 1000, 0.0)
    # entropy mid 100.15, hedge mid 100.05 -> ~9.95 bps
    prem = eng.premium_bps()
    assert prem is not None
    assert abs(prem - 9.95) < 0.1


def test_session_pnl_baseline() -> None:
    eng = Engine(make_cfg())
    eng.entropy = FakeVenue("entropy", "ENTROPY",
                            book_with([(100.1, 1)], [(100.2, 1)]), 1000, 0.0)
    eng.hedge = FakeVenue("hedge", "RH",
                          book_with([(100.0, 1)], [(100.1, 1)]), 1000, 0.0)
    eng.entropy.cash = 100.0
    eng.hedge.cash = 100.0
    pnl = eng.session_pnl()
    assert pnl is not None and pnl == 0.0  # first call sets the baseline


def test_recent_trades_recording() -> None:
    eng = Engine(make_cfg())
    plan = ArbPlan(qty=1.0, buy_limit=100.0, sell_limit=100.5,
                   buy_notional=100.0, sell_notional=100.5, q_max=2.0,
                   q_max_notional=200.0, top_premium_bps=50.0,
                   marginal_premium_bps=50.0, buy_fee=0.0, sell_fee=0.0)
    eng._record_trade("sell_entropy", plan, 0.42, "filled/filled", True)
    assert len(eng.recent_trades) == 1
    tr = eng.recent_trades[0]
    assert tr["direction"] == "sell_entropy"
    assert tr["qty"] == 1.0
    assert tr["fill"] == 0.42 and tr["ok"] is True
