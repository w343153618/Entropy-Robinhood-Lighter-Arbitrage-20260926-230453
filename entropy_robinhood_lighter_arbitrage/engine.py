"""Two-venue arbitrage engine: Entropy vs one hedge venue.

The signal is a fixed band around a configured midline (config.yaml):

    SELL entropy / BUY hedge  when executable premium >= midline + upper (+fees)
    BUY entropy / SELL hedge  when executable premium <= midline - lower (+fees)

Around the signal: per-direction persistence arming,
per-venue inventory ladder + position caps, per-venue order budgets and
reactive rate-limit exclusion, net-delta hedging, venue-outage pausing with
probing, and periodic on-chain reconciliation. There is no paper mode: the
bot either trades live or runs --record-only (data collection, no strategy).
Both venues' books are recorded to 1-minute CSV bars throughout.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import shutil
import time
import uuid
from collections import deque
from pathlib import Path

import aiohttp

from .book import ArbPlan, floor_step, plan_arb
from .config import Config
from .journal import JournalError, OrderJournal
from .recorder import MinuteRecorder
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue

log = logging.getLogger("engine")

CSV_HEADER = ["ts", "direction", "buy_venue", "sell_venue", "qty",
              "buy_limit", "sell_limit", "buy_notional", "sell_notional",
              "exp_edge_usd", "gross_edge_usd", "marginal_premium_bps",
              "midline_bps", "inv_add_bps", "ok", "buy_fill", "sell_fill",
              "buy_status", "sell_status", "fill_edge_usd"]
BALANCE_POLL_SEC = 30.0


class Engine:
    def __init__(self, cfg: Config, record_only: bool = False) -> None:
        self.cfg = cfg
        self.record_only = record_only
        self.session: aiohttp.ClientSession | None = None
        self.entropy = None
        self.hedge = None
        self.venues: dict[str, object] = {}
        self.recorder: MinuteRecorder | None = None
        self.markets_ready = False
        self.stop = asyncio.Event()
        self._update_evt = asyncio.Event()
        self._reconcile_evt = asyncio.Event()
        # per-venue locks: an execution holds both; a reconcile holds one, so
        # a chain read can never race an in-flight order on that venue
        self._venue_locks: dict[str, asyncio.Lock] = {}
        self._exec_tasks: set = set()
        self.halted = False
        self.consec_errors = 0
        self.last_trade_ts = 0.0
        self.trades = 0
        self.hedges = 0
        self.total_exp_edge = 0.0
        self.total_fill_edge = 0.0
        self.start_ts = time.time()
        self._last_skiplog = 0.0
        self._poke_due: float | None = None
        # per-direction persistence arming: direction key -> first-seen ts
        self._armed: dict[str, float | None] = {"sell_entropy": None,
                                                   "buy_entropy": None}
        self._step = 1e-4
        self._min_base = 0.0
        self._min_notional = 10.0
        self._mtm_baseline: float | None = None
        # proactive per-venue send budget: timestamps of recent order sends
        self._sends: dict[str, deque] = {}
        # reactive per-venue throttle: venue key -> excluded until
        self._venue_limited_until: dict[str, float] = {}
        # venue outage tracking: key -> down-since ts; a down venue pauses
        # trading and is probed every venue_probe_sec until it answers
        self._venue_down: dict[str, float] = {}
        self._venue_probe_at: dict[str, float] = {}
        self._venue_fetch_fails: dict[str, int] = {}
        # per-execution records for the dashboard (newest last)
        self.recent_trades: deque = deque(maxlen=50)
        self.journal: OrderJournal | None = None
        self.recovering = False
        self.recovery_reason = ""
        self.halt_reason = ""
        self._recovery_since: float | None = None
        self._recovery_lock = asyncio.Lock()
        self._feed_stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._order_tasks: set[asyncio.Task] = set()
        self._position_checked: dict[str, float] = {}
        self._balance_checked: dict[str, float] = {}
        self._last_order_mono: dict[str, float] = {}
        self._positions_trusted = False
        self._last_trade_mono = 0.0
        self._last_recovery_attempt = 0.0
        self._poke_handle = None
        self._fatal_error = None
        self._shutdown_done = False
        self._startup_recovery = False
        self._blocked_reason = "startup"
        self._disk_checked = -math.inf
        self._disk_free_mb = None

    # ------------------------------------------------------------- utilities

    def _vlock(self, key: str) -> asyncio.Lock:
        lock = self._venue_locks.get(key)
        if lock is None:
            lock = self._venue_locks[key] = asyncio.Lock()
        return lock

    def _venue_rate_ok(self, v, reserve: int = 0) -> bool:
        """True while the venue is under its max_orders_per_min (sliding 60s)."""
        dq = self._sends.setdefault(v.key, deque())
        now = time.monotonic()
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        return len(dq) < v.orders_per_min - reserve

    def _venue_limited(self, v) -> bool:
        return time.monotonic() < self._venue_limited_until.get(v.key, 0.0)

    def _mark_limited(self, v) -> None:
        self._venue_limited_until[v.key] = time.monotonic() + self.cfg.rate_limit_pause_sec
        log.warning("[%s] rate limited — trading paused for %.0fs",
                    v.name, self.cfg.rate_limit_pause_sec)

    def _record_send(self, v) -> None:
        self._sends.setdefault(v.key, deque()).append(time.monotonic())

    def _enter_recovery(self, reason: str) -> None:
        if not self.recovering:
            self._recovery_since = time.monotonic()
            log.warning("RECOVERING: %s", reason)
        self.recovering = True
        self.recovery_reason = reason
        self._armed = dict.fromkeys(self._armed)
        self._reconcile_evt.set()

    def _halt(self, reason: str) -> None:
        self.halted = True
        self.halt_reason = reason
        self._armed = dict.fromkeys(self._armed)
        log.critical("HALTED: %s", reason)
        if self.journal is not None:
            try:
                self.journal.set_meta("halt_reason", reason)
            except Exception:
                log.critical("Could not persist halt marker; durable intents still require recovery")

    def _pending(self) -> list[dict]:
        return self.journal.pending() if self.journal is not None else []

    def _track(self, task: asyncio.Task, collection: set) -> asyncio.Task:
        collection.add(task)
        task.add_done_callback(collection.discard)
        return task

    def _watch_task(self, task: asyncio.Task) -> None:
        def done(t):
            if not self.stop.is_set():
                exc = None if t.cancelled() else t.exception()
                self._fatal_error = f"background task {t.get_name()} stopped ({type(exc).__name__})"
                self._halt(self._fatal_error)
                self.request_stop()
        task.add_done_callback(done)

    def _open_journal(self) -> None:
        identity = {"pair": self.cfg.pair_id, "venues": []}
        for v in self.venues.values():
            if v.kind == "hl":
                account = v._query_address()
                market = [v.api_url, v.coin]
            else:
                account = v.conf.lighter_creds.account_index
                market = [v.profile.api_url, v.market_id]
            identity["venues"].append([v.key, account, market])
        self.journal = OrderJournal(self.cfg.state_db, json.dumps(identity, sort_keys=True))
        self.journal.cancel_prepared()
        self.halt_reason = self.journal.get_meta("halt_reason", "")
        if not isinstance(self.halt_reason, str):
            raise JournalError("invalid persistent halt marker")
        self.halted = bool(self.halt_reason)
        expected = self.journal.get_meta("positions")
        if expected is not None:
            if (not isinstance(expected, dict) or set(expected) != set(self.venues)
                    or any(isinstance(p, bool) or not isinstance(p, (int, float))
                           or not math.isfinite(p) for p in expected.values())):
                raise JournalError("invalid persistent expected positions")
            for key, position in expected.items():
                self.venues[key].position = position
        peak = self.journal.get_meta("equity_high_water")
        if peak is not None and (isinstance(peak, bool) or not isinstance(peak, (int, float))
                                 or not math.isfinite(peak)):
            raise JournalError("invalid persistent equity peak")
        self._startup_recovery = bool(self._pending())
        self._enter_recovery("startup order and position verification")

    def request_stop(self) -> None:
        self.stop.set()
        self._update_evt.set()
        self._reconcile_evt.set()

    # ------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        # Long keepalive so order-path connections survive quiet spells; the
        # keepalive loop pings inside this window to hold them open.
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            keepalive_timeout=75.0, ttl_dns_cache=300))
        try:
            await self._run_inner()
        finally:
            try:
                await self._shutdown()
            finally:
                await self.session.close()
                if self.journal is not None:
                    self.journal.close()
        if self._fatal_error:
            raise RuntimeError(self._fatal_error)

    def _make_venue(self, vc):
        if vc.kind == "lighter":
            return LighterVenue(vc, self.session, self.cfg.settle_timeout_sec,
                                submit_timeout_sec=self.cfg.submit_timeout_sec)
        return HLVenue(vc, self.cfg.hl_api_url, self.cfg.hl_ws_url,
                       self.session, self.cfg.settle_timeout_sec,
                       submit_timeout_sec=self.cfg.submit_timeout_sec)

    async def _run_inner(self) -> None:
        cfg = self.cfg
        self.entropy = self._make_venue(cfg.entropy)
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"entropy": self.entropy, "hedge": self.hedge}
        await asyncio.gather(self.entropy.load_market(), self.hedge.load_market())
        self.markets_ready = True

        live = not self.record_only
        if live:
            if not cfg.creds_complete:
                raise RuntimeError(
                    "live trading needs credentials for both venues in .env "
                    "(see .env.example); use --record-only to run without "
                    "them")
            self.entropy.init_signer()
            self.hedge.init_signer()
            if self.hedge.kind == "hl":
                self.entropy.share_nonces_with(self.hedge)
            self._open_journal()
        if (self.hedge.kind == "hl"
                and self.entropy._query_address()
                and self.entropy._query_address() == self.hedge._query_address()):
            self.hedge.include_core_equity = False  # shared account: count once

        self._step = 10 ** -min(self.entropy.size_decimals,
                                self.hedge.size_decimals)
        self._min_base = max(self.entropy.min_base, self.hedge.min_base,
                             self._step)
        self._min_notional = max(cfg.min_order_notional,
                                 self.entropy.min_quote, self.hedge.min_quote)
        log.info("pair ENTROPY(%s)-%s(%s): midline=%+.2fbps band=[-%.2f, +%.2f] "
                 "fees=%.2f+%.2f step=%g min_ntl=$%g",
                 self.entropy.conf.symbol, self.hedge.name,
                 self.hedge.conf.symbol, cfg.midline_bps, cfg.lower_bps,
                 cfg.upper_bps, self.entropy.fee_bps, self.hedge.fee_bps,
                 self._step, self._min_notional)

        if self.record_only:
            log.warning("RECORD-ONLY — collecting minute data, no strategy, "
                        "no orders")
        else:
            log.warning("LIVE — real orders will be sent (use --record-only "
                        "for credential-less data collection)")
            if not self._pending():
                await self._reconcile_positions(hedge=False, strict=True)
            log.info("starting positions: %s (net %+.6g)",
                     " ".join(f"{v.name}={v.position:+.6g}"
                              for v in self.venues.values()),
                     sum(v.position for v in self.venues.values()))

        tasks = self._tasks
        for v in self.venues.values():
            tasks += v.start_tasks(self._feed_stop, self._update_evt.set, live)
        if cfg.recorder_enabled or self.record_only:
            self.recorder = MinuteRecorder(cfg.recorder_csv, self.entropy.book,
                                           self.hedge.book, cfg.staleness_sec,
                                           pair_id=cfg.pair_id, symbol=cfg.symbol,
                                           entropy_dex=cfg.entropy.hl_dex,
                                           hedge_venue=cfg.hedge_venue,
                                           rotate_daily=cfg.recorder_rotate_daily,
                                           max_pending_rows=cfg.recorder_max_pending_rows)
            tasks.append(asyncio.create_task(self.recorder.run(self.stop),
                                             name="recorder"))
        if not self.record_only:
            tasks.append(asyncio.create_task(self._strategy_loop(),
                                             name="strategy"))
            tasks.append(asyncio.create_task(self._balance_loop(),
                                             name="balances"))
            if cfg.http_keepalive_sec > 0:
                tasks.append(asyncio.create_task(self._http_keepalive_loop(),
                                                 name="keepalive"))
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))
        if live:
            tasks.append(asyncio.create_task(self._reconcile_loop(),
                                             name="reconcile"))
        tasks.append(asyncio.create_task(self._health_loop(), name="health"))
        for task in tasks:
            self._watch_task(task)
        self._update_evt.set()
        await self.stop.wait()

    async def _shutdown(self) -> None:
        if self._shutdown_done:
            return
        self._shutdown_done = True
        self.request_stop()
        if self._poke_handle is not None:
            self._poke_handle.cancel()
        # Account streams remain alive during drain. Every sent order already
        # has a durable intent even if the overall shutdown budget expires.
        active = self._exec_tasks | self._order_tasks
        if active:
            _, pending = await asyncio.wait(active, timeout=self.cfg.shutdown_timeout_sec)
            if pending:
                log.critical("shutdown deadline: %d tasks remain; intents retained for restart", len(pending))
                for task in pending:
                    task.cancel()
                _, stubborn = await asyncio.wait(pending, timeout=1.0)
                if stubborn:
                    self._fatal_error = "order task ignored cancellation; supervisor must stop process"
                    log.critical(self._fatal_error)
        self._feed_stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            done, _ = await asyncio.wait(self._tasks, timeout=1.0)
            for task in done:
                if not task.cancelled():
                    task.exception()
        closes = [asyncio.create_task(v.close()) for v in self.venues.values()]
        if closes:
            done, pending = await asyncio.wait(closes, timeout=2.0)
            for task in pending:
                task.cancel()
            for task in done:
                if not task.cancelled():
                    task.exception()
        try:
            self._write_health(stopped=True)
        except Exception:
            log.error("could not write stopped health status; monitoring must reject stale health")
        log.info("shutdown — %d trades, %d hedges, exp edge $%.4f, "
                 "fill edge $%.4f", self.trades, self.hedges,
                 self.total_exp_edge, self.total_fill_edge)

    # --------------------------------------------------------------- signals

    def _inv_add_bps(self, buy, sell) -> float:
        """Inventory ladder: a surcharge that grows once a venue's position
        passes floor_frac of its cap in the direction the trade would add to
        (buying adds when that venue is >= flat long; selling adds when the
        venue is <= flat short). Max of the two venues' ramps."""
        scale = self.cfg.inventory_scale_bps
        if scale <= 0:
            return 0.0
        floor = min(max(self.cfg.inventory_floor_frac, 0.0), 0.99)

        def ramp(v, adding: bool) -> float:
            if not adding:
                return 0.0
            ref = v.book.mid()
            if ref is None:
                return 0.0
            u = min(abs(v.position) * ref / v.cap_usd, 1.0)
            if u <= floor:
                return 0.0
            return scale * (u - floor) / (1.0 - floor)

        return max(ramp(buy, buy.position >= 0), ramp(sell, sell.position <= 0))

    def _eff_threshold(self, buy, sell) -> float:
        """Net hurdle (bps, on top of fees) for the direction buy->sell.

        selling entropy: executable premium must clear midline + upper;
        buying entropy: the reverse premium must clear lower - midline."""
        if sell.key == "entropy":
            base = self.cfg.midline_bps + self.cfg.upper_bps
        else:
            base = self.cfg.lower_bps - self.cfg.midline_bps
        return base + self._inv_add_bps(buy, sell)

    def _headroom(self, buy, sell, ref_px: float) -> float:
        hb = buy.cap_usd - buy.position * ref_px
        hs = sell.cap_usd + sell.position * ref_px
        return min(hb, hs)

    def _plan(self, buy, sell, cap_notional: float):
        slip = self.cfg.leg_slippage_bps / 1e4
        # Reserve the allowed loss on BOTH legs before deciding there is an
        # opportunity. The configured band can intentionally be negative.
        hurdle = ((1 + self._eff_threshold(buy, sell) / 1e4)
                  * (1 + slip) / (1 - slip) - 1) * 1e4
        buy_ref = buy.book.best_ask()
        sell_ref = sell.book.best_ask()
        if not buy_ref or not sell_ref:
            return None, "empty_book"
        room = min(buy.cap_usd / (buy_ref * (1 + slip)) - buy.position,
                   sell.cap_usd / (sell_ref * (1 + slip)) + sell.position)
        plan, reason = plan_arb(
            buy.book, sell.book,
            threshold_bps=hurdle,
            buy_fee_bps=buy.fee_bps, sell_fee_bps=sell.fee_bps,
            take_fraction=self.cfg.take_fraction,
            cap_notional=cap_notional / (1 + slip),
            min_base=self._min_base,
            min_notional=self._min_notional,
            size_step=self._step,
            max_base=max(room, 0.0),
        )
        if plan is None:
            return None, reason
        # Deep asks can cost more than the top-of-book reference. Re-size once
        # using the worst visited price, then independently verify the result.
        room = min(buy.cap_usd / (max(buy_ref, plan.buy_limit) * (1 + slip)) - buy.position,
                   sell.cap_usd / (max(sell_ref, plan.sell_limit) * (1 + slip)) + sell.position)
        if plan.qty > room + 1e-12:
            plan, reason = plan_arb(
                buy.book, sell.book, threshold_bps=hurdle,
                buy_fee_bps=buy.fee_bps, sell_fee_bps=sell.fee_bps,
                take_fraction=self.cfg.take_fraction,
                cap_notional=cap_notional / (1 + slip), min_base=self._min_base,
                min_notional=self._min_notional, size_step=self._step,
                max_base=max(room, 0.0))
        return plan, reason

    def _bounds(self, buy, sell, plan):
        slip = self.cfg.leg_slippage_bps / 1e4
        bp = buy.px_round(plan.buy_limit * (1 + slip), round_up=False)
        sp = sell.px_round(plan.sell_limit * (1 - slip), round_up=True)
        hurdle = 1 + self._eff_threshold(buy, sell) / 1e4
        if sp * (1 - plan.sell_fee) + 1e-12 < bp * (1 + plan.buy_fee) * hurdle:
            return None
        if bp < plan.buy_limit or sp > plan.sell_limit:
            return None
        for v, new_position, ref in (
                (buy, buy.position + plan.qty, max(bp, buy.book.best_ask() or bp)),
                (sell, sell.position - plan.qty, max(plan.sell_limit, sell.book.best_ask() or sp))):
            # An already-over-cap position may still be reduced.
            if (abs(new_position) > abs(v.position) + 1e-12
                    and abs(new_position) * ref > v.cap_usd + 1e-8):
                return None
        if max(bp * plan.qty, plan.sell_notional) > self.cfg.max_order_notional + 1e-8:
            return None
        return bp, sp

    # -------------------------------------------------------------- strategy

    async def _strategy_loop(self) -> None:
        while not self.stop.is_set():
            await self._update_evt.wait()
            self._update_evt.clear()
            if self.stop.is_set():
                break
            try:
                await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate failed")
                self._halt("strategy evaluation failed")

    def _schedule_poke(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time() + max(delay, 0.01)
        if self._poke_due is not None and self._poke_due <= due + 0.02:
            return

        def _fire() -> None:
            self._poke_due = None
            if not self.stop.is_set():
                self._update_evt.set()

        if self._poke_handle is not None:
            self._poke_handle.cancel()
        self._poke_due = due
        self._poke_handle = loop.call_at(due, _fire)

    def _skiplog(self, fmt: str, *args) -> None:
        now = time.time()
        if now - self._last_skiplog >= 2.0:
            self._last_skiplog = now
            log.info(fmt, *args)

    async def _evaluate(self) -> None:
        cfg = self.cfg
        if self.halted or self.stop.is_set() or not self._opening_allowed():
            return
        now = time.monotonic()
        if now - self._last_trade_mono < cfg.cooldown_sec:
            self._schedule_poke(cfg.cooldown_sec - (now - self._last_trade_mono))
            return
        best = self._scan(now)
        if best is None:
            return
        buy, sell, plan = best
        # _scan verified both locks free and nothing ran since (no awaits),
        # so these acquires take the no-suspension fast path
        await self._vlock(buy.key).acquire()
        await self._vlock(sell.key).acquire()
        # run as a task so a shutdown cancels the strategy loop's await, never
        # the in-flight execution itself (both legs must settle)
        t = asyncio.create_task(self._execute_locked(buy, sell, plan))
        self._track(t, self._exec_tasks)
        await asyncio.shield(t)

    async def _execute_locked(self, buy, sell, plan: ArbPlan) -> None:
        """Run one execution while holding both venue locks (acquired by the
        caller), then release them and settle the aftermath: unresolved
        outcomes escalate to reconcile, everything else gets a net-delta
        check."""
        unresolved = True
        self._enter_recovery("execution in progress")
        try:
            unresolved = await self._execute(buy, sell, plan)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("execute failed")
            self._halt("execution failed; inspect durable order journal")
        finally:
            self._vlock(buy.key).release()
            self._vlock(sell.key).release()
        if not unresolved and not self._pending():
            await self._maybe_hedge()
        if not self._pending() and self._net_ok() and self._positions_trusted:
            self._clear_recovery()
        self._update_evt.set()  # freed venues may have a queued opportunity

    def _scan(self, now: float):
        """Evaluate both directions; returns the best executable
        (buy, sell, plan), or None."""
        cfg = self.cfg
        if self.stop.is_set() or self.halted or self.recovering or self._pending():
            return None
        best = None
        for buy, sell, dkey in ((self.hedge, self.entropy, "sell_entropy"),
                                (self.entropy, self.hedge, "buy_entropy")):
            if not (buy.book.is_fresh(cfg.staleness_sec)
                    and sell.book.is_fresh(cfg.staleness_sec)):
                self._armed[dkey] = None
                continue
            if not (buy.ready_to_trade() and sell.ready_to_trade()):
                self._armed[dkey] = None
                continue
            if self._venue_down:
                self._armed[dkey] = None
                continue  # a venue in outage pauses the (only) pair
            if self._vlock(buy.key).locked() or self._vlock(sell.key).locked():
                continue  # mid-execution or mid-reconcile
            if self._venue_limited(buy) or self._venue_limited(sell):
                self._armed[dkey] = None
                continue  # reactive 429 exclusion
            if not (self._venue_rate_ok(buy, reserve=2) and self._venue_rate_ok(sell, reserve=2)):
                self._skiplog("%s deferred: venue order budget exhausted", dkey)
                continue
            # never refire into books that predate the venue's own last trade
            if (buy.book.last_update_ts <= buy.last_traded_ts
                    or sell.book.last_update_ts <= sell.last_traded_ts):
                continue
            plan, reason = self._plan(buy, sell, cfg.max_order_notional)
            edge_present = reason not in ("no_edge", "empty_book")
            if not edge_present:
                self._armed[dkey] = None
                continue
            armed = self._armed.get(dkey)
            if armed is None:
                # premium persistence: only fire if the edge survives
                # premium_persist_sec (filters one-tick phantoms)
                self._armed[dkey] = now
                self._schedule_poke(cfg.premium_persist_sec)
                continue
            if now - armed < cfg.premium_persist_sec:
                self._schedule_poke(cfg.premium_persist_sec - (now - armed))
                continue
            if plan is None:
                continue
            if self._bounds(buy, sell, plan) is None:
                continue
            if best is None or plan.exp_edge_usd > best[2].exp_edge_usd:
                best = (buy, sell, plan)
        return best

    # ------------------------------------------------------------- execution

    def _intent(self, venue, is_buy, qty, limit_px, reduce_only, batch_id):
        return {"id": venue.new_client_order_id(), "venue": venue.key,
                "is_buy": is_buy, "qty": qty, "limit_px": limit_px,
                "reduce_only": reduce_only, "batch_id": batch_id}

    @staticmethod
    def _validated_result(intent, info):
        try:
            if not isinstance(info, dict) or not isinstance(info.get("unresolved"), bool):
                raise ValueError("missing certainty")
            if isinstance(info["filled_base"], bool) or isinstance(info.get("avg_px"), bool):
                raise ValueError("boolean order quantities are invalid")
            fill = float(info["filled_base"])
            px = info.get("avg_px")
            if not math.isfinite(fill) or fill < 0 or fill > intent["qty"] + 1e-9:
                raise ValueError("invalid fill")
            if px is not None:
                px = float(px)
                if not math.isfinite(px) or px <= 0:
                    raise ValueError("invalid fill price")
            if not isinstance(info.get("status"), str):
                raise ValueError("missing status")
            return {**info, "filled_base": fill, "avg_px": px,
                    "client_order_id": intent["id"]}
        except (KeyError, TypeError, ValueError):
            return {"status": "invalid-response", "filled_base": 0.0,
                    "avg_px": None, "err": "untrusted order response",
                    "unresolved": True, "client_order_id": intent["id"]}

    def _accept_result(self, v, intent, result, from_lookup=False):
        result = self._validated_result(intent, result)
        first_terminal = self.journal.record_result(intent["id"], result)
        if first_terminal:
            fill = result["filled_base"]
            expected = self.journal.get_meta("positions")
            if expected is not None:
                v.position = expected[v.key]
            else:
                raise JournalError("order result has no verified position baseline")
            if fill:
                px = result.get("avg_px") or intent["limit_px"]
                fee = v.fee_bps / 1e4
                v.cash += -fill * px * (1 + fee) if intent["is_buy"] else fill * px * (1 - fee)
                v.volume_usd += fill * px
        if first_terminal or not from_lookup:
            v.last_traded_ts = time.time()
            self._last_order_mono[v.key] = time.monotonic()
        if str(result.get("err", "")).startswith("RATE_LIMITED"):
            self._mark_limited(v)
        if result["unresolved"]:
            self._positions_trusted = False
            self._enter_recovery("order outcome unknown")
        return result

    async def _send_order(self, v, intent):
        self.journal.mark_submitted(intent["id"])
        self._record_send(v)
        started = time.monotonic()
        try:
            info = await asyncio.wait_for(v.send_taker(
                is_buy=intent["is_buy"], qty=intent["qty"], limit_px=intent["limit_px"],
                reduce_only=intent["reduce_only"], client_order_id=intent["id"]),
                timeout=self.cfg.submit_timeout_sec + self.cfg.settle_timeout_sec + 1)
        except asyncio.CancelledError:
            self._accept_result(v, intent, {"status": "interrupted", "filled_base": 0.0,
                                "avg_px": None, "err": None, "unresolved": True})
            raise
        except Exception as exc:
            info = {"status": "unknown", "filled_base": 0.0, "avg_px": None,
                    "err": type(exc).__name__, "unresolved": True}
        info = self._accept_result(v, intent, info)
        log.info("[ORDER] id=%s venue=%s status=%s fill=%g latency_ms=%.1f",
                 intent["id"], v.key, info["status"], info["filled_base"],
                 (time.monotonic() - started) * 1000)
        return info

    async def _execute(self, buy, sell, plan: ArbPlan) -> bool:
        """Send both legs and settle the fills. Both venue locks are held by
        the caller. Returns True when an outcome is unresolved and the caller
        must escalate to reconcile."""
        if self.halted or self.stop.is_set():
            return False
        if self.journal is None or not self._positions_trusted:
            raise JournalError("orders require a durable, verified position baseline")
        cfg = self.cfg
        bounds = self._bounds(buy, sell, plan)
        if bounds is None:
            return False
        if not (buy.book.is_fresh(cfg.staleness_sec) and sell.book.is_fresh(cfg.staleness_sec)):
            return False
        inv_bps = self._inv_add_bps(buy, sell)
        direction = "sell_entropy" if sell.key == "entropy" else "buy_entropy"
        self.last_trade_ts = time.time()
        log.info("[ARB] %s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
                 "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f",
                 direction, buy.name, plan.qty, plan.buy_limit, sell.name,
                 plan.sell_limit, plan.buy_notional, plan.q_max_notional,
                 plan.marginal_premium_bps, plan.exp_edge_usd)
        buy_bound, sell_bound = bounds
        batch_id = uuid.uuid4().hex
        intents = [self._intent(buy, True, plan.qty, buy_bound, False, batch_id),
                   self._intent(sell, False, plan.qty, sell_bound, False, batch_id)]
        self.journal.prepare_batch(intents)
        res = await asyncio.gather(
            *(self._track(asyncio.create_task(self._send_order(v, i)), self._order_tasks)
              for v, i in zip((buy, sell), intents)),
            return_exceptions=True)
        if any(isinstance(r, BaseException) for r in res):
            self._halt("order outcome could not be durably recorded")
        binfo, sinfo = (r if isinstance(r, dict) else
                        {"status": "unknown", "filled_base": 0.0,
                         "avg_px": None, "err": type(r).__name__, "unresolved": True}
                        for r in res)
        for v, info, side in ((buy, binfo, "buy"), (sell, sinfo, "sell")):
            if info.get("err"):
                log.error("[%s] %s leg: %s", v.name, side, info["err"])
        bfill = binfo["filled_base"]
        sfill = sinfo["filled_base"]

        matched = min(bfill, sfill)
        fill_edge = 0.0
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            fill_edge = matched * (sinfo["avg_px"] * (1 - plan.sell_fee)
                                   - binfo["avg_px"] * (1 + plan.buy_fee))
            self.total_fill_edge += fill_edge
        log.info("[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
                 "matched %.6g | fill edge $%.4f", direction,
                 buy.name, binfo["status"], bfill, plan.qty,
                 sell.name, sinfo["status"], sfill, plan.qty, matched, fill_edge)
        buy.last_traded_ts = sell.last_traded_ts = time.time()

        unresolved = binfo.get("unresolved") or sinfo.get("unresolved")
        hard_err = (binfo.get("err") is not None
                    or sinfo.get("err") is not None)
        rate_limited = False
        for v, info in ((buy, binfo), (sell, sinfo)):
            if str(info.get("err", "")).startswith("RATE_LIMITED"):
                rate_limited = True
                self._mark_limited(v)
            elif "margin" in str(info.get("status", "")).lower():
                log.warning("[%s] margin rejection — collateral exhausted, "
                            "pausing venue", v.name)
                self._mark_limited(v)
        sent_ok = not hard_err and not unresolved
        if sent_ok:
            self.consec_errors = 0
        elif not rate_limited:
            self.consec_errors += 1
            if self.consec_errors >= cfg.max_consecutive_errors:
                self._halt(f"{self.consec_errors} consecutive execution problems")
        if sent_ok and matched > 0:
            self.trades += 1
            self.total_exp_edge += plan.exp_edge_usd
        self._record_trade(direction, plan,
                           None if unresolved else fill_edge,
                           f"{binfo['status']}/{sinfo['status']}", sent_ok)
        self._log_csv(direction, buy, sell, plan, sent_ok, bfill, sfill,
                      binfo["status"], sinfo["status"], fill_edge, inv_bps)
        self.last_trade_ts = time.time()
        self._last_trade_mono = time.monotonic()
        return bool(unresolved)

    def _record_trade(self, direction: str, plan: ArbPlan, fill_edge,
                      status: str, ok: bool) -> None:
        self.recent_trades.append({
            "ts": time.time(), "direction": direction, "qty": plan.qty,
            "notional": plan.buy_notional,
            "prem_bps": plan.marginal_premium_bps,
            "exp": plan.exp_edge_usd, "fill": fill_edge, "status": status,
            "ok": ok})

    def _net_usd(self) -> float | None:
        prices = [v.book.mid() for v in self.venues.values()]
        if not prices or any(p is None or not math.isfinite(p) for p in prices):
            return None
        return abs(sum(v.position for v in self.venues.values())) * max(prices)

    def _hedge_candidate(self, net, executable=False):
        is_sell = net > 0
        for v in sorted(self.venues.values(), key=lambda v: -v.position * (1 if is_sell else -1)):
            if v.position * net <= 0:
                continue
            if executable and (v.key in self._venue_down or self._venue_limited(v)
                               or not self._venue_rate_ok(v) or not v.ready_to_trade()
                               or not v.book.is_fresh(self.cfg.staleness_sec)
                               or v.book.last_update_ts <= v.last_traded_ts):
                continue
            ref = v.book.best_bid() if is_sell else v.book.best_ask()
            if ref is None:
                continue
            # Recovery uses THIS venue's precision, not the coarser pair step.
            step = 10 ** -getattr(v, "size_decimals", 4)
            qty = floor_step(min(abs(net), abs(v.position)), step)
            slip = self.cfg.hedge_slippage_bps / 1e4
            limit = v.px_round(ref * (1 - slip if is_sell else 1 + slip), not is_sell)
            qty = floor_step(min(qty, self.cfg.max_order_notional / max(ref, limit)), step)
            if qty >= max(v.min_base, step) and qty * limit >= v.min_quote:
                return v, qty, limit, not is_sell
        return None

    def _net_ok(self) -> bool:
        net = abs(sum(v.position for v in self.venues.values()))
        if net <= 1e-12:
            return True
        usd = self._net_usd()
        if usd is None:
            return False
        if net <= self.cfg.net_tolerance_base and usd <= self.cfg.net_tolerance_usd:
            return True
        # Only an intrinsically untradeable residue can use the dust budget.
        fresh = all(v.book.is_fresh(self.cfg.staleness_sec) for v in self.venues.values())
        return (fresh and usd <= self.cfg.max_dust_usd
                and self._hedge_candidate(sum(v.position for v in self.venues.values())) is None)

    def _clear_recovery(self) -> None:
        if self._pending() or not self._positions_trusted or not self._net_ok():
            return
        if self.recovering:
            log.info("recovery complete: positions verified, net=%+.8g", sum(v.position for v in self.venues.values()))
        self.recovering = False
        self.recovery_reason = ""
        self._recovery_since = None
        self._startup_recovery = False
        self._update_evt.set()

    def _opening_allowed(self) -> bool:
        self._blocked_reason = ""
        if self.halted or self.stop.is_set():
            self._blocked_reason = self.halt_reason or "stopping"
            return False
        if self.journal is None or self.recovering or not self._positions_trusted or self._pending():
            self._blocked_reason = self.recovery_reason or "orders/positions not verified"
            return False
        if not self._net_ok():
            self._enter_recovery("net exposure exceeds tolerance")
            self._blocked_reason = self.recovery_reason
            return False
        now = time.monotonic()
        for v in self.venues.values():
            if now - self._position_checked.get(v.key, -math.inf) > self.cfg.position_staleness_sec:
                self._enter_recovery("position verification is stale")
                self._blocked_reason = self.recovery_reason
                return False
            if (now - self._balance_checked.get(v.key, -math.inf) > self.cfg.balance_staleness_sec
                    or v.free is None or v.free < self.cfg.min_free_collateral_usd):
                self._blocked_reason = "collateral unavailable, stale, or below reserve"
                return False
            if not v.book.is_fresh(self.cfg.staleness_sec) or not v.ready_to_trade():
                self._armed = dict.fromkeys(self._armed)
                self._blocked_reason = "market or settlement feed not ready"
                return False
            if self._venue_limited(v) or not self._venue_rate_ok(v, reserve=2):
                self._blocked_reason = "venue rate budget reserved for recovery"
                self._schedule_poke(1.0)
                return False
        if now - self._disk_checked > 5:
            self._disk_free_mb = shutil.disk_usage(Path(self.journal.path).parent).free / 1024 ** 2
            self._disk_checked = now
        if self._disk_free_mb < self.cfg.min_disk_free_mb:
            self._halt("disk reserve exhausted")
            self._blocked_reason = self.halt_reason
            return False
        return True

    async def _maybe_hedge(self) -> None:
        if self.stop.is_set() or self._pending() or not self._positions_trusted:
            return
        if self._net_ok():
            self._clear_recovery()
            return
        self._enter_recovery("net exposure needs reduction")
        await self._hedge(sum(v.position for v in self.venues.values()))

    async def _hedge(self, net: float) -> None:
        if self.stop.is_set() or self._pending() or not self._positions_trusted:
            return
        candidate = self._hedge_candidate(net, executable=True)
        if candidate is None:
            return  # Keep recovering; an excessive untradeable residue cannot reopen.
        v, qty, limit, is_buy = candidate
        if (v.key in self._venue_down or self._venue_limited(v)
                or not self._venue_rate_ok(v) or not v.ready_to_trade()
                or not v.book.is_fresh(self.cfg.staleness_sec)
                or v.book.last_update_ts <= v.last_traded_ts):
            return
        # Normal strategy sends stop two orders before the venue limit, leaving
        # room for recovery. Recovery itself still respects the actual budget.
        lock = self._vlock(v.key)
        if lock.locked():
            return
        await lock.acquire()
        try:
            if self.journal is None:
                raise JournalError("recovery requires a durable order journal")
            intent = self._intent(v, is_buy, qty, limit, True, uuid.uuid4().hex)
            self.journal.prepare_batch([intent])
            self.hedges += 1
            log.warning("[HEDGE] net=%+.8g venue=%s qty=%g id=%s", net, v.key, qty, intent["id"])
            task = self._track(asyncio.create_task(self._send_order(v, intent)), self._order_tasks)
            await asyncio.shield(task)
        finally:
            lock.release()
        if self._net_ok() and not self._pending():
            self._clear_recovery()
        else:
            self._enter_recovery("hedge partial, unfilled, or unresolved")

    # --------------------------------------------------- reconcile / status

    async def _resolve_pending(self) -> None:
        for intent in self._pending():
            if intent["state"] == "prepared":
                # Only possible after a local failure BEFORE sending either leg.
                self.journal.cancel_prepared()
                continue
            v = self.venues[intent["venue"]]
            try:
                info = await asyncio.wait_for(v.resolve_order(intent["id"]),
                                              timeout=self.cfg.settle_timeout_sec + 1)
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
            self._accept_result(v, intent, info, from_lookup=True)

    async def _reconcile_positions(self, hedge: bool, strict: bool = False) -> None:
        # Serialize the WHOLE pair snapshot. Per-venue snapshots must never be
        # mixed with an intervening order on the other venue.
        async with self._recovery_lock:
            locks = [self._vlock(k) for k in sorted(self.venues)]
            if any(lock.locked() for lock in locks):
                return
            for lock in locks:
                await lock.acquire()
            try:
                if self._pending():
                    self._enter_recovery("resolving durable pending orders")
                    await self._resolve_pending()
                    if self._pending():
                        return
                now = time.monotonic()
                if any(now - self._last_order_mono.get(v.key, -math.inf)
                       < self.cfg.reconcile_grace_sec for v in self.venues.values()):
                    return  # Recovery loop retries after its bounded poll interval.
                results = await asyncio.gather(
                    *(asyncio.wait_for(v.fetch_position(), timeout=self.cfg.submit_timeout_sec + 1)
                      for v in self.venues.values()), return_exceptions=True)
                actual = {}
                for v, r in zip(self.venues.values(), results):
                    if (isinstance(r, (BaseException, bool))
                            or not isinstance(r, (int, float)) or not math.isfinite(r)):
                        self._positions_trusted = False
                        self._venue_down.setdefault(v.key, time.time())
                        self._enter_recovery("authoritative position read failed")
                        if strict:
                            raise RuntimeError(f"[{v.name}] cannot verify starting position")
                        return
                    actual[v.key] = float(r)
                expected = self.journal.get_meta("positions") if self.journal else None
                if expected is not None and any(abs(actual[k] - expected[k]) > 1e-9 for k in actual):
                    # A stale REST read must NEVER undo a confirmed fill. External
                    # trades/liquidations also require intervention, not guessing.
                    self._positions_trusted = False
                    self._enter_recovery("exchange positions differ from durable expected positions")
                    return
                if self.journal is not None and expected is None:
                    self.journal.set_meta("positions", actual)
                for v in self.venues.values():
                    v.position = actual[v.key]
                    self._position_checked[v.key] = time.monotonic()
                self._positions_trusted = True
                self._venue_down.clear()
                self._clear_recovery()
            finally:
                for lock in reversed(locks):
                    lock.release()
        if hedge:
            await self._maybe_hedge()

    def _check_recovery_deadline(self):
        if (self.recovering and self._recovery_since is not None
                and time.monotonic() - self._recovery_since > self.cfg.recovery_timeout_sec
                and not self.halted):
            self._halt("recovery deadline exceeded; unresolved orders/positions retained")

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            retry_floor = (max(self.cfg.venue_probe_sec, self.cfg.recovery_poll_sec)
                           if self.halted else self.cfg.recovery_poll_sec)
            delay = (retry_floor if self.recovering or self.halted
                     else self.cfg.reconcile_sec)
            try:
                await asyncio.wait_for(self._reconcile_evt.wait(), timeout=delay)
            except TimeoutError:
                pass
            self._reconcile_evt.clear()
            if self.stop.is_set():
                break
            # An unknown result can set the event again. Bound retries so an
            # exchange error cannot create a request storm.
            remaining = retry_floor - (time.monotonic() - self._last_recovery_attempt)
            if remaining > 0:
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=remaining)
                    break
                except TimeoutError:
                    pass
            self._last_recovery_attempt = time.monotonic()
            try:
                # Confirmed partial fills can be reduced using trusted local
                # state without waiting for eventually consistent REST reads.
                if self.recovering:
                    await self._maybe_hedge()
                await self._reconcile_positions(hedge=True)
                self._check_recovery_deadline()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("recovery failed")
                self._halt("recovery could not safely persist or verify state")

    async def _poll_balances(self) -> None:
        results = await asyncio.gather(
            *(asyncio.wait_for(v.fetch_equity(), timeout=self.cfg.submit_timeout_sec + 1)
              for v in self.venues.values()), return_exceptions=True)
        valid = True
        for v, got in zip(self.venues.values(), results):
            if (isinstance(got, BaseException) or not isinstance(got, (tuple, list)) or len(got) != 2
                    or any(isinstance(x, bool) or not isinstance(x, (int, float))
                           or not math.isfinite(x) for x in got)):
                valid = False
                self._balance_checked.pop(v.key, None)
                continue
            v.equity, v.free = got
            self._balance_checked[v.key] = time.monotonic()
            if v.start_equity is None:
                v.start_equity = v.equity
        if valid and self.journal is not None:
            equity = sum(v.equity for v in self.venues.values())
            peak = self.journal.get_meta("equity_high_water")
            if peak is None or equity > peak:
                peak = equity
                self.journal.set_meta("equity_high_water", peak)
            if peak - equity >= self.cfg.max_session_loss_usd:
                self._halt("trading-account equity drawdown limit reached")
        self._update_evt.set()

    async def _balance_loop(self) -> None:
        while not self.stop.is_set():
            await self._poll_balances()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=BALANCE_POLL_SEC)
            except TimeoutError:
                pass

    def _write_health(self, stopped=False):
        if not self.venues:
            return
        can_open = False if stopped or self.record_only else self._opening_allowed()
        state = ("STOPPED" if stopped else "RECORD_ONLY" if self.record_only else
                 "HALTED" if self.halted else "RECOVERING" if self.recovering else
                 "READY" if can_open else "PAUSED")
        now = time.monotonic()
        payload = {"schema_version": 1, "ts": time.time(), "pair_id": self.cfg.pair_id,
                   "state": state, "stopped": stopped, "halt_reason": self.halt_reason,
                   "recovery_reason": self.recovery_reason,
                   "opening_allowed": can_open, "blocked_reason": self._blocked_reason,
                   "free_collateral": {k: v.free for k, v in self.venues.items()},
                   "disk_free_mb": self._disk_free_mb,
                   "pending_orders": len(self._pending()),
                   "net_base": sum(v.position for v in self.venues.values()),
                   "net_usd": self._net_usd(),
                   "positions": {k: v.position for k, v in self.venues.items()},
                   "books_fresh": {k: v.book.is_fresh(self.cfg.staleness_sec) for k, v in self.venues.items()},
                   "position_age_sec": {k: now - self._position_checked[k] if k in self._position_checked else None for k in self.venues},
                   "balance_age_sec": {k: now - self._balance_checked[k] if k in self._balance_checked else None for k in self.venues}}
        if self.recorder is not None:
            payload["recorder"] = {k: getattr(self.recorder, k) for k in
                                   ("pending_rows", "write_errors", "last_write_error", "paused_samples")}
            payload["recorder"]["healthy"] = not bool(self.recorder.last_write_error)
        path = Path(self.cfg.health_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, allow_nan=False), encoding="utf-8")
        os.replace(tmp, path)

    async def _health_loop(self) -> None:
        while not self.stop.is_set():
            self._write_health()
            # Check even during silence; a feed outage cannot indefinitely hide
            # an unknown order or prevent recovery deadline alarms.
            self._check_recovery_deadline()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=5)
            except TimeoutError:
                pass

    async def _http_keepalive_loop(self) -> None:
        if self.cfg.http_keepalive_sec <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.http_keepalive_sec)
                return
            except TimeoutError:
                pass
            await asyncio.gather(*(v.warm_http() for v in self.venues.values()),
                                 return_exceptions=True)

    def account_delta(self) -> float | None:
        """Change in real account equity since start (both venues)."""
        total = 0.0
        for v in self.venues.values():
            if v.equity is None or v.start_equity is None:
                return None
            total += v.equity - v.start_equity
        return total

    def session_pnl(self) -> float | None:
        total = 0.0
        for v in self.venues.values():
            m = v.book.mid()
            if m is None:
                return None
            total += v.cash + v.position * m
        if self._mtm_baseline is None:
            self._mtm_baseline = total
        return total - self._mtm_baseline

    def premium_bps(self) -> float | None:
        em, hm = self.entropy.book.mid(), self.hedge.book.mid()
        if not (em and hm):
            return None
        return (em / hm - 1.0) * 1e4

    async def _status_loop(self) -> None:
        cfg = self.cfg
        while not self.stop.is_set():
            try:
                await asyncio.sleep(cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            books = " | ".join(
                f"{v.name} {v.book.best_bid() or '—'}/{v.book.best_ask() or '—'}"
                + ("" if v.book.is_fresh(cfg.staleness_sec) else " STALE")
                + (" RATE-LTD" if self._venue_limited(v) else "")
                + (" DOWN" if v.key in self._venue_down else "")
                for v in self.venues.values())
            prem = self.premium_bps()
            prem_s = f"{prem:+.2f}" if prem is not None else "—"
            pos = " ".join(f"{v.name} {v.position:+.6g}"
                           for v in self.venues.values())
            net = sum(v.position for v in self.venues.values())
            pnl = self.session_pnl()
            rec = (f" | rec {self.recorder.rows_written} rows"
                   if self.recorder else "")
            log.info("[status] %s | prem %s bps (band %+.2f..%+.2f) | pos %s "
                     "net %+.6g | trades %d hedges %d | MTM %s expEdge $%.4f "
                     "fillEdge $%.4f%s%s",
                     books, prem_s, cfg.midline_bps - cfg.lower_bps,
                     cfg.midline_bps + cfg.upper_bps, pos, net, self.trades,
                     self.hedges,
                     f"${pnl:+.4f}" if pnl is not None else "—",
                     self.total_exp_edge, self.total_fill_edge, rec,
                     " *** HALTED ***" if self.halted else "")

    def _log_csv(self, direction, buy, sell, plan: ArbPlan, ok: bool, bfill,
                 sfill, bstatus, sstatus, fill_edge, inv_bps) -> None:
        try:
            base = Path(self.cfg.trades_csv)
            day = time.strftime("%Y-%m-%d", time.gmtime())
            path = str(base.with_name(f"{base.stem}-{day}{base.suffix}"))
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            if os.path.exists(path):
                with open(path) as fh0:
                    if fh0.readline().strip() != ",".join(CSV_HEADER):
                        os.rename(path, path + f".old-{time.time_ns()}")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(CSV_HEADER)
                w.writerow([f"{time.time():.3f}",
                            direction, buy.name, sell.name, f"{plan.qty:.8g}",
                            plan.buy_limit, plan.sell_limit,
                            f"{plan.buy_notional:.2f}", f"{plan.sell_notional:.2f}",
                            f"{plan.exp_edge_usd:.4f}", f"{plan.gross_edge_usd:.4f}",
                            f"{plan.marginal_premium_bps:.3f}",
                            f"{self.cfg.midline_bps:.3f}",
                            f"{inv_bps:.3f}", int(ok), f"{bfill:.8g}",
                            f"{sfill:.8g}", bstatus, sstatus, f"{fill_edge:.4f}"])
                fh.flush()
                os.fsync(fh.fileno())
        except Exception:
            log.exception("csv write failed")
