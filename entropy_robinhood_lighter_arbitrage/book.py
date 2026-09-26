"""Order book state and fee-aware arbitrage sizing.

One book class serves both feed protocols: zkLighter sends a snapshot plus
diffs (dict maintenance), Hyperliquid's l2Book sends full snapshots.
HL freshness requires market updates. Lighter's diff protocol may be quiet,
but connection liveness is bounded by a periodic authoritative snapshot.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal

Level = tuple[float, float]


class OrderBook:
    def __init__(self, freshness_mode: str = "market",
                 snapshot_refresh_sec: float = 60.0) -> None:
        if freshness_mode not in ("market", "connection"):
            raise ValueError("unknown book freshness mode")
        if not math.isfinite(snapshot_refresh_sec) or snapshot_refresh_sec <= 0:
            raise ValueError("snapshot_refresh_sec must be positive and finite")
        self.freshness_mode = freshness_mode
        self.snapshot_refresh_sec = snapshot_refresh_sec
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.ready = False
        self.last_update_ts = 0.0
        self.alive_ts = 0.0

    def _validate(self) -> bool:
        self.ready = bool(self.bids and self.asks) and max(self.bids) < min(self.asks)
        return self.ready

    @staticmethod
    def _level(px, sz) -> tuple[float, float]:
        px, sz = float(px), float(sz)
        if not math.isfinite(px) or not math.isfinite(sz) or px <= 0 or sz < 0:
            raise ValueError("invalid price or size")
        return px, sz

    def touch(self) -> None:
        self.alive_ts = time.time()

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.ready = False
        self.last_update_ts = 0.0

    # ---- zkLighter snapshot + diff ----
    def apply_lighter(self, ob: dict, snapshot: bool) -> bool:
        if not isinstance(ob, dict):
            self.clear()
            return False
        if snapshot:
            self.bids.clear()
            self.asks.clear()
        try:
            for name, side in (("bids", self.bids), ("asks", self.asks)):
                for lvl in ob.get(name) or []:
                    px, sz = self._level(lvl["price"], lvl["size"])
                    if sz == 0:
                        side.pop(px, None)
                    else:
                        side[px] = sz
        except (TypeError, ValueError, KeyError, OverflowError):
            self.clear()
            return False
        if not self._validate():
            self.last_update_ts = 0.0
            return False
        self.last_update_ts = time.time()
        self.touch()
        return True

    # ---- Hyperliquid full snapshot ----
    def apply_hl(self, levels: list, timestamp_ms: float | None = None,
                 max_age_sec: float = 10.0) -> bool:
        now = time.time()
        try:
            if len(levels) != 2:
                raise ValueError("expected two book sides")
            stamp = now if timestamp_ms is None else float(timestamp_ms) / 1000
            if not math.isfinite(stamp) or not -1.0 <= now - stamp <= max_age_sec:
                raise ValueError("stale or future exchange snapshot")
            stamp = min(stamp, now)  # bound tolerated sub-second exchange clock skew
            sides = []
            for side in levels:
                parsed = [self._level(l["px"], l["sz"]) for l in side]
                sides.append({px: sz for px, sz in parsed if sz > 0})
            self.bids, self.asks = sides
        except (TypeError, ValueError, KeyError, OverflowError):
            self.clear()
            return False
        if not self._validate():
            self.last_update_ts = 0.0
            return False
        self.last_update_ts = stamp
        self.touch()
        return True

    def sorted_bids(self) -> list[Level]:
        return sorted(self.bids.items(), key=lambda kv: -kv[0])

    def sorted_asks(self) -> list[Level]:
        return sorted(self.asks.items())

    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def mid(self) -> float | None:
        if not (self.bids and self.asks):
            return None
        return (max(self.bids) + min(self.asks)) / 2.0

    def is_fresh(self, max_age_sec: float) -> bool:
        if not math.isfinite(max_age_sec) or max_age_sec <= 0:
            return False
        now = time.time()
        market_max_age = (max_age_sec if self.freshness_mode == "market"
                          else self.snapshot_refresh_sec)
        return self.ready and bool(self.bids) and bool(self.asks) and (
            0 <= now - self.alive_ts <= max_age_sec
            and 0 <= now - self.last_update_ts <= market_max_age)


def floor_step(x: float, step: float) -> float:
    if not math.isfinite(x) or not math.isfinite(step) or x < 0 or step <= 0:
        raise ValueError("quantity and step must be finite and nonnegative/positive")
    quantum = Decimal(str(step))
    units = (Decimal(str(x)) / quantum).to_integral_value(rounding=ROUND_FLOOR)
    result = float(units * quantum)
    # Decimal-to-float rounding must never exceed the caller's actual bound.
    while result > x:
        units -= 1
        result = float(units * quantum)
    return max(result, 0.0)


def crossable_base(asks: list[Level], bids: list[Level], threshold: float,
                   buy_fee: float = 0.0, sell_fee: float = 0.0) -> tuple[float, float]:
    """Walk both books level by level and return (base qty, buy notional) that
    can be crossed while every marginal slice still clears fees + threshold."""
    qty = 0.0
    buy_notional = 0.0
    i = j = 0
    a_px = a_rem = 0.0
    b_px = b_rem = 0.0
    while True:
        if a_rem <= 0:
            if i >= len(asks):
                break
            a_px, a_rem = asks[i]
            i += 1
        if b_rem <= 0:
            if j >= len(bids):
                break
            b_px, b_rem = bids[j]
            j += 1
        if b_px * (1.0 - sell_fee) < a_px * (1.0 + buy_fee) * (1.0 + threshold):
            break
        take = min(a_rem, b_rem)
        qty += take
        buy_notional += take * a_px
        a_rem -= take
        b_rem -= take
    return qty, buy_notional


def walk_depth(levels: list[Level], qty: float) -> tuple[float, float]:
    remaining = qty
    notionals = []
    marginal_px = levels[0][0]
    for px, sz in levels:
        take = min(remaining, sz)
        notionals.append(take * px)
        marginal_px = px
        remaining -= take
        if remaining <= 1e-12:
            break
    return marginal_px, math.fsum(notionals)


def _base_for_notional(levels: list[Level], cap: float) -> float:
    remaining = cap
    quantities = []
    for px, sz in levels:
        take = min(sz, max(remaining, 0.0) / px)
        quantities.append(take)
        remaining -= take * px
        if take < sz or remaining <= 0:
            break
    return math.fsum(quantities)


@dataclass
class ArbPlan:
    qty: float
    buy_limit: float
    sell_limit: float
    buy_notional: float
    sell_notional: float
    q_max: float
    q_max_notional: float
    top_premium_bps: float
    marginal_premium_bps: float
    buy_fee: float
    sell_fee: float

    @property
    def gross_edge_usd(self) -> float:
        return self.sell_notional - self.buy_notional

    @property
    def exp_edge_usd(self) -> float:
        return (self.sell_notional * (1.0 - self.sell_fee)
                - self.buy_notional * (1.0 + self.buy_fee))


def plan_arb(buy_book: OrderBook, sell_book: OrderBook, *, threshold_bps: float,
             buy_fee_bps: float, sell_fee_bps: float, take_fraction: float,
             cap_notional: float, min_base: float, min_notional: float,
             size_step: float, max_base: float | None = None):
    """Size a two-leg taker slice: buy on buy_book, sell on sell_book.

    A slice qualifies when the executable premium (sell bid over buy ask)
    clears both venues' taker fees plus threshold_bps. Returns
    (ArbPlan | None, reason).
    """
    inputs = (threshold_bps, buy_fee_bps, sell_fee_bps, take_fraction,
              cap_notional, min_base, min_notional, size_step)
    if (not all(math.isfinite(x) for x in inputs)
            or threshold_bps <= -1e4 or buy_fee_bps < 0
            or not 0 <= sell_fee_bps < 1e4 or not 0 < take_fraction <= 1
            or cap_notional <= 0 or min_base < 0 or min_notional < 0
            or size_step <= 0 or (max_base is not None
                                 and (not math.isfinite(max_base) or max_base < 0))):
        return None, "invalid_input"
    asks = buy_book.sorted_asks()
    bids = sell_book.sorted_bids()
    if not asks or not bids:
        return None, "empty_book"
    if any(not math.isfinite(px) or not math.isfinite(sz) or px <= 0 or sz <= 0
           for px, sz in asks + bids):
        return None, "invalid_book"
    threshold = threshold_bps / 1e4
    buy_fee = buy_fee_bps / 1e4
    sell_fee = sell_fee_bps / 1e4
    top_premium_bps = (bids[0][0] / asks[0][0] - 1.0) * 1e4
    if bids[0][0] * (1.0 - sell_fee) < asks[0][0] * (1.0 + buy_fee) * (1.0 + threshold):
        return None, "no_edge"
    q_max, q_max_notional = crossable_base(asks, bids, threshold, buy_fee, sell_fee)
    if not math.isfinite(q_max) or not math.isfinite(q_max_notional):
        return None, "invalid_book"
    if q_max <= 0:
        return None, "no_edge"
    target = min(q_max * take_fraction, _base_for_notional(asks, cap_notional),
                 _base_for_notional(bids, cap_notional),
                 max_base if max_base is not None else q_max)
    target = floor_step(target, size_step)
    if target <= 0 or target < min_base:
        return None, "below_min_base"
    buy_limit, buy_notional = walk_depth(asks, target)
    sell_limit, sell_notional = walk_depth(bids, target)
    while buy_notional > cap_notional or sell_notional > cap_notional:
        target = floor_step(max(target - size_step, 0.0), size_step)
        if target <= 0 or target < min_base:
            return None, "below_min_base"
        buy_limit, buy_notional = walk_depth(asks, target)
        sell_limit, sell_notional = walk_depth(bids, target)
    if buy_notional < min_notional or sell_notional < min_notional:
        return None, "below_min_notional"
    return ArbPlan(
        qty=target, buy_limit=buy_limit, sell_limit=sell_limit,
        buy_notional=buy_notional, sell_notional=sell_notional,
        q_max=q_max, q_max_notional=q_max_notional,
        top_premium_bps=top_premium_bps,
        marginal_premium_bps=(sell_limit / buy_limit - 1.0) * 1e4,
        buy_fee=buy_fee, sell_fee=sell_fee,
    ), "ok"
