"""Order book + fee-aware arbitrage sizing math (entropy_robinhood_lighter_arbitrage.book)."""
from __future__ import annotations

from entropy_robinhood_lighter_arbitrage.book import (
    OrderBook,
    crossable_base,
    floor_step,
    plan_arb,
    walk_depth,
)


def hl_book(bids, asks) -> OrderBook:
    b = OrderBook()
    b.apply_hl([[{"px": px, "sz": sz} for px, sz in bids],
                [{"px": px, "sz": sz} for px, sz in asks]])
    return b


def test_apply_hl_full_snapshot() -> None:
    b = hl_book([(100.0, 5.0), (99.9, 3.0)], [(100.1, 4.0), (100.2, 2.0)])
    assert b.best_bid() == 100.0
    assert b.best_ask() == 100.1
    assert b.mid() == 100.05
    assert len(b.bids) == 2 and len(b.asks) == 2
    assert b.is_fresh(10.0)


def test_apply_lighter_snapshot_then_diff() -> None:
    b = OrderBook()
    b.apply_lighter({"bids": [{"price": "100.0", "size": "5"}],
                     "asks": [{"price": "100.1", "size": "4"}]}, snapshot=True)
    assert b.best_bid() == 100.0 and b.best_ask() == 100.1
    # diff: remove best bid (size 0), tighten ask
    b.apply_lighter({"bids": [{"price": "100.0", "size": "0"}],
                     "asks": [{"price": "100.05", "size": "4"}]}, snapshot=False)
    assert b.best_bid() is None  # bids side fully removed
    assert b.best_ask() == 100.05
    assert b.bids == {}
    # one-sided book: not fresh (is_fresh requires both sides non-empty)
    assert b.ready is False
    assert b.is_fresh(10.0) is False


def test_floor_step() -> None:
    assert floor_step(0.123456, 0.01) == 0.12
    assert floor_step(5.0, 0.1) == 5.0
    assert floor_step(0.000123, 0.0001) == 0.0001


def test_crossable_base_fees_threshold() -> None:
    asks = [(100.0, 2.0), (100.2, 2.0)]
    bids = [(100.4, 2.0), (100.5, 2.0)]
    # no fees/threshold: everything crosses
    q, _ = crossable_base(asks, bids, 0.0)
    assert q == 4.0
    # threshold 50 bps: only top levels cross (100.4 vs 100.0 = 40 bps < 50)
    q2, _ = crossable_base(asks, bids, 50.0)
    assert q2 == 0.0
    # fees eat the edge: sell fee 40 bps kills the top level
    q3, _ = crossable_base(asks, bids, 0.0, sell_fee=0.004)
    assert q3 == 0.0


def test_walk_depth() -> None:
    levels = [(100.0, 1.0), (100.1, 1.0), (100.2, 1.0)]
    marginal, notional = walk_depth(levels, 2.5)
    assert marginal == 100.2
    assert notional == 100.0 + 100.1 + 100.2 * 0.5


def test_plan_arb_qualifies() -> None:
    buy = hl_book([], [(100.0, 10.0)])
    sell = hl_book([(100.5, 10.0)], [])
    plan, reason = plan_arb(
        buy, sell, threshold_bps=10.0, buy_fee_bps=0.0, sell_fee_bps=0.0,
        take_fraction=0.5, cap_notional=1000.0, min_base=0.01,
        min_notional=10.0, size_step=0.01)
    assert reason == "ok"
    assert plan is not None
    assert plan.qty == 5.0                      # half of 10
    assert plan.buy_limit == 100.0
    assert plan.sell_limit == 100.5
    assert plan.exp_edge_usd > 0.0


def test_plan_arb_no_edge() -> None:
    buy = hl_book([], [(100.0, 10.0)])
    sell = hl_book([(100.05, 10.0)], [])       # only 5 bps
    plan, reason = plan_arb(
        buy, sell, threshold_bps=10.0, buy_fee_bps=0.0, sell_fee_bps=0.0,
        take_fraction=0.5, cap_notional=1000.0, min_base=0.01,
        min_notional=10.0, size_step=0.01)
    assert plan is None and reason == "no_edge"


def test_plan_arb_fees_consume_edge() -> None:
    buy = hl_book([], [(100.0, 10.0)])
    sell = hl_book([(100.2, 10.0)], [])        # 20 bps top premium
    plan, reason = plan_arb(
        buy, sell, threshold_bps=0.0, buy_fee_bps=10.0, sell_fee_bps=10.0,
        take_fraction=0.5, cap_notional=1000.0, min_base=0.01,
        min_notional=10.0, size_step=0.01)
    # 20 bps gross minus 20 bps fees = 0 -> no edge
    assert plan is None and reason == "no_edge"


def test_plan_arb_min_notional() -> None:
    buy = hl_book([], [(100.0, 0.05)])
    sell = hl_book([(100.5, 0.05)], [])
    plan, reason = plan_arb(
        buy, sell, threshold_bps=0.0, buy_fee_bps=0.0, sell_fee_bps=0.0,
        take_fraction=1.0, cap_notional=1000.0, min_base=0.0001,
        min_notional=10.0, size_step=0.0001)
    assert plan is None and reason == "below_min_notional"


def test_multilevel_cap_applies_to_both_actual_notionals() -> None:
    buy = hl_book([], [(100.0, 1.0), (110.0, 20.0)])
    sell = hl_book([(130.0, 20.0)], [])
    plan, reason = plan_arb(buy, sell, threshold_bps=0, buy_fee_bps=0,
                            sell_fee_bps=0, take_fraction=1, cap_notional=500,
                            min_base=.01, min_notional=10, size_step=.01)
    assert reason == 'ok'
    assert plan.buy_notional <= 500 and plan.sell_notional <= 500


def test_plan_optional_base_cap_and_ieee_boundary() -> None:
    buy = hl_book([], [(.1, 10000)])
    sell = hl_book([(.11, 10000)], [])
    plan, _ = plan_arb(buy, sell, threshold_bps=0, buy_fee_bps=0,
                        sell_fee_bps=0, take_fraction=1, cap_notional=100,
                        min_base=.01, min_notional=.001, size_step=.01,
                        max_base=.29999999999999993)
    assert plan.qty <= .29999999999999993
    assert floor_step(.29999999999999993, .1) <= .29999999999999993


def test_invalid_books_never_become_fresh() -> None:
    import pytest
    for bids, asks in [([(float('nan'), 1)], [(101, 1)]),
                        ([(100, float('inf'))], [(101, 1)]),
                        ([(102, 1)], [(101, 1)]), ([], [(101, 1)])]:
        book = hl_book(bids, asks)
        assert not book.is_fresh(10)
        assert not book.ready
    with pytest.raises(ValueError):
        floor_step(1, 0)


def test_connection_liveness_cannot_refresh_hl_market() -> None:
    import time
    book = hl_book([(100, 1)], [(101, 1)])
    book.last_update_ts = time.time() - 100
    book.touch()
    assert not book.is_fresh(10)


def test_plan_rejects_nonfinite_sizing() -> None:
    buy = hl_book([], [(100, 10)])
    sell = hl_book([(101, 10)], [])
    for bad in [float('nan'), float('inf'), -1]:
        plan, reason = plan_arb(buy, sell, threshold_bps=0, buy_fee_bps=0,
                                sell_fee_bps=0, take_fraction=1, cap_notional=bad,
                                min_base=.01, min_notional=10, size_step=.01)
        assert plan is None and reason == 'invalid_input'


def test_randomized_depth_caps_and_base_bounds() -> None:
    import math
    import random
    rng = random.Random(20260926)
    for _ in range(500):
        prices = sorted(rng.uniform(10, 100) for _ in range(5))
        asks = [(px, rng.uniform(.1, 20)) for px in prices]
        bids = [(px + 110, rng.uniform(.1, 20)) for px in reversed(prices)]
        buy = hl_book([], asks)
        sell = hl_book(bids, [])
        cap = rng.uniform(20, 2000)
        base_cap = rng.uniform(.1, 20)
        plan, _ = plan_arb(buy, sell, threshold_bps=0, buy_fee_bps=0,
                           sell_fee_bps=0, take_fraction=rng.uniform(.1, 1),
                           cap_notional=cap, min_base=.0001, min_notional=.1,
                           size_step=.0001, max_base=base_cap)
        if plan is not None:
            assert plan.qty <= base_cap
            assert plan.buy_notional <= cap and plan.sell_notional <= cap
            assert plan.qty <= math.fsum(sz for _, sz in asks)
            assert plan.qty <= math.fsum(sz for _, sz in bids)
            assert plan.exp_edge_usd > 0
