"""Official exchange feeds with fail-closed books and bounded resubscription."""
from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Callable

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook

log = logging.getLogger("feeds")


def _chan_id(channel: str) -> int | None:
    """'order_book:32' / 'order_book/32' -> 32."""
    if not isinstance(channel, str):
        return None
    for sep in (":", "/"):
        if sep in channel:
            try:
                return int(channel.rsplit(sep, 1)[1])
            except ValueError:
                return None
    return None


async def _wait_retry(stop: asyncio.Event, backoff: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), min(30.0, backoff * random.uniform(.8, 1.2)))
    except TimeoutError:
        pass


async def _stop_socket(ws, stop: asyncio.Event) -> None:
    await stop.wait()
    await ws.close()


async def _cancel_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


class LighterBookFeed:
    """Strict nonce continuity; quiet diff feeds get fresh snapshots every minute."""

    def __init__(self, name: str, ws_url: str, market_id: int, book: OrderBook,
                 notify: Callable[[], None], snapshot_refresh_sec: float = 60.0) -> None:
        import math
        if not math.isfinite(snapshot_refresh_sec) or snapshot_refresh_sec <= 0:
            raise ValueError("snapshot_refresh_sec must be positive and finite")
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.book = book
        self.book.freshness_mode = "connection"
        self.book.snapshot_refresh_sec = snapshot_refresh_sec
        self.notify = notify
        self._nonce: int | None = None
        self._synced = False
        self.last_error: str | None = None

    async def _subscribe(self, ws) -> None:
        await ws.send(json.dumps({"type": "subscribe",
                                  "channel": f"order_book/{self.market_id}"}))

    async def _reset_snapshot(self, ws, reason: str) -> None:
        log.warning("[%s] %s — requesting authoritative snapshot", self.name, reason)
        self._nonce = None
        self._synced = False
        self.book.clear()
        self.notify()
        await ws.send(json.dumps({"type": "unsubscribe",
                                  "channel": f"order_book/{self.market_id}"}))
        await self._subscribe(ws)

    @staticmethod
    def _valid_nonce(value) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0

    async def _handle_book(self, ws, msg: dict, snapshot: bool) -> None:
        if _chan_id(msg.get("channel", "")) != self.market_id:
            return
        ob = msg.get("order_book")
        if not isinstance(ob, dict) or not self._valid_nonce(ob.get("nonce")):
            await self._reset_snapshot(ws, "missing/invalid book nonce")
            return
        end = ob["nonce"]
        if snapshot:
            if not self.book.apply_lighter(ob, snapshot=True):
                await self._reset_snapshot(ws, "invalid snapshot")
                return
            self._nonce = end
            self._synced = True
            log.info("[%s] snapshot: %d bids / %d asks", self.name,
                     len(self.book.bids), len(self.book.asks))
            self.notify()
            return
        if not self._synced:
            return
        begin, prev = ob.get("begin_nonce"), self._nonce
        if not self._valid_nonce(begin) or begin > end:
            await self._reset_snapshot(ws, "missing/invalid diff interval")
            return
        # Fully applied intervals are idempotent, including delayed duplicates.
        # Partly overlapping intervals cannot prove continuity and are rejected.
        if end <= prev:
            return
        if begin != prev:
            await self._reset_snapshot(ws, f"diff discontinuity (had {prev}, got {begin})")
            return
        if not self.book.apply_lighter(ob, snapshot=False):
            await self._reset_snapshot(ws, "invalid diff book")
            return
        self._nonce = end
        self.notify()

    async def _refresh_loop(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(self.book.snapshot_refresh_sec)
                await self._reset_snapshot(ws, "periodic snapshot refresh")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = repr(exc)
            log.warning("[%s] snapshot refresh failed: %s", self.name, exc)
            self.book.clear()
            self.notify()
            await ws.close()

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        try:
            while not stop.is_set():
                helpers = []
                try:
                    async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                          ping_interval=15, ping_timeout=15) as ws:
                        log.info("[%s] connected (%s)", self.name, self.ws_url)
                        self.book.clear()
                        self._nonce = None
                        self._synced = False
                        helpers = [asyncio.create_task(self._refresh_loop(ws)),
                                   asyncio.create_task(_stop_socket(ws, stop))]
                        async for raw in ws:
                            msg = json.loads(raw)
                            self.book.touch()
                            t = msg.get("type")
                            if t == "update/order_book":
                                await self._handle_book(ws, msg, snapshot=False)
                            elif t == "subscribed/order_book":
                                await self._handle_book(ws, msg, snapshot=True)
                            elif t == "connected":
                                await self._subscribe(ws)
                            elif t == "ping":
                                await ws.send(json.dumps({"type": "pong"}))
                            elif t == "error" or (isinstance(t, str)
                                    and t.startswith("unsubscribed")
                                    and _chan_id(msg.get("channel", "")) == self.market_id):
                                self.book.clear()
                                self._synced = False
                                self.notify()
                                # An expected unsubscribe during refresh is followed
                                # by subscribe; an exchange error needs reconnect.
                                if t == "error":
                                    raise RuntimeError("exchange subscription error")
                            if self.book.ready:
                                backoff = 1.0
                            if stop.is_set():
                                break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = repr(exc)
                    log.warning("[%s] ws error: %s — reconnect in ~%.0fs",
                                self.name, exc, backoff)
                finally:
                    self.book.clear()
                    self._synced = False
                    self._nonce = None
                    self.notify()
                    await _cancel_tasks(helpers)
                if not stop.is_set():
                    await _wait_retry(stop, backoff)
                    backoff = min(backoff * 2, 30.0)
        finally:
            self.book.clear()
            self.notify()
            log.info("[%s] book feed stopped", self.name)


class HLBookFeed:
    """Full snapshots require fresh exchange time; pongs prove only connectivity."""

    def __init__(self, name: str, ws_url: str, coin: str, book: OrderBook,
                 notify: Callable[[], None], ping_sec: float = 5.0,
                 max_snapshot_age_sec: float = 10.0) -> None:
        self.name = name
        self.ws_url = ws_url
        self.coin = coin
        self.book = book
        self.book.freshness_mode = "market"
        self.notify = notify
        self.ping_sec = ping_sec
        self.max_snapshot_age_sec = max_snapshot_age_sec
        self._snapped = False
        self.last_error: str | None = None

    def _on_frame(self, msg: dict, require_timestamp: bool = False) -> None:
        self.book.touch()
        channel = msg.get("channel")
        if channel == "error":
            self.book.clear()
            self.notify()
            raise RuntimeError("exchange subscription error")
        if channel == "subscriptionResponse":
            data = msg.get("data") or {}
            subscription = data.get("subscription") or {}
            if data.get("method") == "unsubscribe" and subscription.get("coin") == self.coin:
                self.book.clear()
                self.notify()
            return
        if channel != "l2Book":
            return
        d = msg.get("data") or {}
        if d.get("coin") != self.coin:
            return
        if require_timestamp and d.get("time") is None:
            self.book.clear()
            self.notify()
            raise ValueError("exchange snapshot missing timestamp")
        if not self.book.apply_hl(d.get("levels"), timestamp_ms=d.get("time"),
                                  max_age_sec=self.max_snapshot_age_sec):
            self.notify()
            return
        if not self._snapped:
            self._snapped = True
            log.info("[%s] snapshot: %d bids / %d asks", self.name,
                     len(self.book.bids), len(self.book.asks))
        self.notify()

    async def _pinger(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(self.ping_sec)
                await ws.send(json.dumps({"method": "ping"}))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = repr(exc)
            log.warning("[%s] ping failed: %s", self.name, exc)
            self.book.clear()
            self.notify()
            await ws.close()

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        try:
            while not stop.is_set():
                helpers = []
                try:
                    async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                          ping_interval=15, ping_timeout=15) as ws:
                        log.info("[%s] connected (official ws, %s)", self.name, self.coin)
                        self.book.clear()
                        self._snapped = False
                        await ws.send(json.dumps({"method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": self.coin}}))
                        helpers = [asyncio.create_task(self._pinger(ws)),
                                   asyncio.create_task(_stop_socket(ws, stop))]
                        async for raw in ws:
                            self._on_frame(json.loads(raw), require_timestamp=True)
                            if self.book.ready:
                                backoff = 1.0
                            if stop.is_set():
                                break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = repr(exc)
                    log.warning("[%s] ws error: %s — reconnect in ~%.0fs",
                                self.name, exc, backoff)
                finally:
                    self.book.clear()
                    self.notify()
                    await _cancel_tasks(helpers)
                if not stop.is_set():
                    await _wait_retry(stop, backoff)
                    backoff = min(backoff * 2, 30.0)
        finally:
            self.book.clear()
            self.notify()
            log.info("[%s] book feed stopped", self.name)
