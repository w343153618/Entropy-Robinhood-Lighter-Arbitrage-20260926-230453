"""zkLighter venue adapter (Lighter mainnet / Lighter Robinhood chain).

Market data and account state come from Lighter's public REST + websocket
APIs via plain aiohttp/websockets, so --record-only data collection works
without the SDK. Trading lazily imports the official `lighter` SDK
(https://github.com/elliottech/lighter-python) for transaction signing only.

Market orders carry mandatory avg-execution-price protection and settle
asynchronously on the authenticated account_orders websocket; send_taker()
hides that behind the same result shape the HL venue returns:
{status, filled_base, avg_px, err, unresolved}.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import secrets
import time
from collections import OrderedDict
from itertools import count

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import VenueConf
from .feeds import LighterBookFeed

log = logging.getLogger("lighter")

OPEN_STATUSES = {"in-progress", "pending", "open"}
TERMINAL_STATUSES = {
    "filled", "canceled", "canceled-post-only", "canceled-reduce-only",
    "canceled-position-not-allowed", "canceled-margin-not-allowed",
    "canceled-too-much-slippage", "canceled-not-enough-liquidity",
    "canceled-self-trade", "canceled-expired", "canceled-oco", "canceled-child",
    "canceled-liquidation", "canceled-invalid-balance",
}
AUTH_REFRESH_SEC = 8 * 60
REST_TIMEOUT = 10.0
ACCOUNT_FRESH_SEC = 90.0
# One sequence per process, shared across venue objects. A random lower-half
# seed avoids clock/reset collisions without the birthday risk of drawing a
# new 48-bit random ID for every order. Journal uniqueness spans restarts.
_CLIENT_ORDER_IDS = count(secrets.randbelow((1 << 47) - 1) + 1)


def _finite(value, *, nonnegative: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not an amount")
    number = float(value)
    if not math.isfinite(number) or (nonnegative and number < 0):
        raise ValueError("invalid amount")
    return number


def _terminal_info(order: dict) -> dict | None:
    """Only recognized terminal states with trustworthy fill amounts settle."""
    if not isinstance(order, dict) or order.get("status") not in TERMINAL_STATUSES:
        return None
    try:
        fb = _finite(order["filled_base_amount"], nonnegative=True)
        fq = _finite(order["filled_quote_amount"], nonnegative=True)
        if (fb == 0) != (fq == 0) or (order["status"] == "filled" and fb == 0):
            return None
        if "initial_base_amount" in order:
            initial = _finite(order["initial_base_amount"], nonnegative=True)
            if fb > initial + 1e-12:
                return None
        avg = fq / fb if fb > 0 else None
        if avg is not None and not math.isfinite(avg):
            return None
        return {"status": order["status"], "filled_base": fb,
                "filled_quote": fq, "avg_px": avg}
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


class AccountOrdersFeed:
    """Authenticated stream of our own order updates (settlement channel)."""

    def __init__(self, name: str, ws_url: str, market_id: int,
                 account_index: int, signer) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.account_index = account_index
        self.signer = signer
        self.ready = asyncio.Event()
        self.alive_ts = 0.0
        self._pending: dict[int, asyncio.Future] = {}
        self._terminal: OrderedDict[int, dict] = OrderedDict()

    def watch(self, coi: int) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        if coi in self._terminal:
            fut.set_result(self._terminal[coi])
            return fut
        self._pending[coi] = fut
        return fut

    def unwatch(self, coi: int) -> None:
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def _resolve(self, coi: int, info: dict) -> None:
        self._terminal[coi] = info
        while len(self._terminal) > 512:
            self._terminal.popitem(last=False)
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.set_result(info)

    def _handle_orders(self, msg: dict) -> None:
        orders = msg.get("orders")
        if not isinstance(orders, dict):
            return
        for market, lst in orders.items():
            # The channel is scoped to one market. Reject an unexpected route.
            try:
                if int(market) != self.market_id:
                    continue
            except (TypeError, ValueError):
                continue
            if not isinstance(lst, list):
                continue
            for o in lst or []:
                info = _terminal_info(o)
                if info is None:
                    continue
                try:
                    coi = int(o.get("client_order_index"))
                except (TypeError, ValueError):
                    continue
                if "market_index" in o and o["market_index"] != self.market_id:
                    continue
                if "owner_account_index" in o and o["owner_account_index"] != self.account_index:
                    continue
                self._resolve(coi, info)

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            self.ready.clear()
            try:
                auth, err = self.signer.create_auth_token_with_expiry()
                if err is not None:
                    raise RuntimeError(f"auth token: {err}")
                connected_at = time.time()
                async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                      ping_interval=15, ping_timeout=15) as ws:
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        self.alive_ts = time.monotonic()
                        t = msg.get("type")
                        if t in ("subscribed/account_orders", "update/account_orders"):
                            if t.startswith("subscribed"):
                                log.info("[%s] account orders stream ready", self.name)
                            if not isinstance(msg.get("orders"), dict):
                                continue
                            self.ready.set()
                            self._handle_orders(msg)
                        elif t == "connected":
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": f"account_orders/{self.market_id}/"
                                           f"{self.account_index}",
                                "auth": auth}))
                        elif t == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        if stop.is_set():
                            break
                        if time.time() - connected_at > AUTH_REFRESH_SEC:
                            log.info("[%s] refreshing account ws auth", self.name)
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] account ws error: %s — retry in %.0fs",
                            self.name, e, backoff)
                self.ready.clear()
                if stop.is_set():
                    break
                await asyncio.sleep(backoff * random.uniform(0.8, 1.2))
                backoff = min(backoff * 2, 30.0)
                continue
            finally:
                self.ready.clear()
            if not stop.is_set():
                await asyncio.sleep(backoff * random.uniform(0.8, 1.2))
                backoff = min(backoff * 2, 30.0)


class LighterVenue:
    kind = "lighter"

    def __init__(self, conf: VenueConf, session: aiohttp.ClientSession,
                 settle_timeout_sec: float, submit_timeout_sec: float = 5.0) -> None:
        assert conf.lighter_profile is not None
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.submit_timeout = submit_timeout_sec
        self.profile = conf.lighter_profile
        self.book = OrderBook()
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0     # cumulative filled notional this session
        self.equity = None
        self.free = None
        self.start_equity = None
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.market_id = -1
        self.price_decimals = 2
        self.size_decimals = 4
        self.min_base = 0.0
        self.min_quote = 10.0
        self.signer = None
        self.orders_feed: AccountOrdersFeed | None = None

    # ------------------------------------------------------------------ REST

    async def _get(self, path: str, params: dict | None = None,
                   headers: dict | None = None):
        async with self.session.get(
                self.profile.api_url + path, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
            r.raise_for_status()
            return await r.json()

    # ------------------------------------------------------------- lifecycle

    async def load_market(self) -> None:
        data = await self._get("/api/v1/orderBooks")
        if not isinstance(data, dict) or data.get("code") != 200 \
                or not isinstance(data.get("order_books"), list):
            raise RuntimeError(f"[{self.name}] invalid market metadata")
        for ob in data["order_books"]:
            if not isinstance(ob, dict):
                raise RuntimeError(f"[{self.name}] invalid market record")
            if ob.get("symbol") != self.conf.symbol:
                continue
            if ob.get("status") != "active":
                raise RuntimeError(f"[{self.name}] market status={ob.get('status')}")
            try:
                self.market_id = int(ob["market_id"])
                self.price_decimals = int(ob["supported_price_decimals"])
                self.size_decimals = int(ob["supported_size_decimals"])
                self.min_base = _finite(ob["min_base_amount"], nonnegative=True)
                self.min_quote = _finite(ob["min_quote_amount"], nonnegative=True)
                if "multiplier" in ob and _finite(ob["multiplier"], nonnegative=True) != 1:
                    raise ValueError("unsupported contract multiplier")
                if not 0 <= self.market_id < 255 or not 0 <= self.price_decimals <= 8 \
                        or not 0 <= self.size_decimals <= 8 or self.min_base <= 0 \
                        or self.min_quote <= 0 or ob.get("market_type", "perp") != "perp":
                    raise ValueError("unsupported market constraints")
            except (KeyError, TypeError, ValueError) as e:
                raise RuntimeError(f"[{self.name}] invalid market constraints") from e
            if "taker_fee" in ob:
                # API fee strings are percentages. This public market floor
                # does not prove Standard/Premium/Plus account-specific fees.
                market_fee = _finite(ob["taker_fee"], nonnegative=True) * 100
                self.fee_bps = max(self.fee_bps, market_fee)
            log.warning("[%s] account fee tier unverified; configured fee floor %.4g bps",
                        self.name, self.fee_bps)
            log.info("[%s] %s market_id=%d px_dec=%d sz_dec=%d min_base=%s "
                     "min_quote=%s taker_fee=%s", self.name, ob["symbol"],
                     self.market_id, self.price_decimals, self.size_decimals,
                     ob["min_base_amount"], ob["min_quote_amount"],
                     ob.get("taker_fee"))
            return
        raise RuntimeError(f"[{self.name}] {self.conf.symbol} not found on "
                           f"{self.profile.name}")

    def init_signer(self) -> None:
        c = self.conf.lighter_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from lighter import SignerClient
        except ImportError as e:
            raise RuntimeError(
                "live trading on Lighter needs the official SDK — "
                "first run python scripts/build_lighter_wheel.py, then "
                "pip install -r requirements-live.txt") from e
        signer = SignerClient(
            url=self.profile.api_url,
            account_index=c.account_index,
            api_private_keys={c.api_key_index: c.api_private_key},
            chain_id=self.profile.chain_id,
        )
        # Own the client before validation so Engine's finally path also closes
        # its HTTP session when the key check fails.
        self.signer = signer
        err = signer.check_client()
        if err is not None:
            raise RuntimeError(f"[{self.name}] API key check failed: {err}")
        log.info("[%s] signer ready (account %d)", self.name, c.account_index)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            LighterBookFeed(self.name, self.profile.ws_url, self.market_id,
                            self.book, notify).run(stop),
            name=f"book-{self.key}")]
        if live:
            self.orders_feed = AccountOrdersFeed(
                self.name, self.profile.ws_url, self.market_id,
                self.conf.lighter_creds.account_index, self.signer)
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        return (self.orders_feed is not None and self.orders_feed.ready.is_set()
                and time.monotonic() - self.orders_feed.alive_ts <= ACCOUNT_FRESH_SEC)

    async def warm_http(self) -> None:
        """Keep the order-path HTTPS connections warm (a cold TLS handshake
        adds 10-15ms to the first order after an idle spell)."""
        try:
            await self._get("/api/v1/status")
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)
        if self.signer is None:
            return
        try:
            sess = self.signer.api_client.rest_client.pool_manager
            async with sess.get(self.profile.api_url + "/api/v1/status",
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
                await r.read()
        except Exception as e:
            log.debug("[%s] signer keepalive failed: %r", self.name, e)

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        f = 10 ** self.price_decimals
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def new_client_order_id(self) -> str:
        """Lighter accepts nonzero unsigned 48-bit client order indices."""
        coi = next(_CLIENT_ORDER_IDS)
        if coi >= 1 << 48:
            raise RuntimeError("client order sequence exhausted")
        return str(coi)

    @staticmethod
    def _result(client_order_id: str, status: str = "unknown", *,
                filled_base: float = 0.0, avg_px: float | None = None,
                err: str | None = None, unresolved: bool = True) -> dict:
        return {"client_order_id": client_order_id, "status": status,
                "filled_base": filled_base, "avg_px": avg_px,
                "err": err, "unresolved": unresolved}

    async def resolve_order(self, client_order_id: str) -> dict:
        """Resolve WS cache or authenticated REST; absence is never proof of failure.

        accountOrders covers recent orders only (1K inactive / 24h per SDK).
        Fall back to active orders and a bounded cursor walk through history.
        Retention, unavailable APIs and exhausted pages leave the journal pending.
        """
        try:
            coi = int(client_order_id)
            if not 0 < coi < (1 << 48):
                raise ValueError("invalid client order index")
        except (TypeError, ValueError):
            return self._result(client_order_id, err="invalid client order index")
        if self.orders_feed and coi in self.orders_feed._terminal:
            info = self.orders_feed._terminal[coi]
            return self._result(client_order_id, info["status"],
                                filled_base=info["filled_base"],
                                avg_px=info.get("avg_px"), unresolved=False)
        try:
            return await asyncio.wait_for(self._resolve_rest(client_order_id),
                                          timeout=self.settle_timeout)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._result(client_order_id, err=f"order lookup: {type(e).__name__}")

    async def _resolve_rest(self, client_order_id: str) -> dict:
        if self.signer is None or self.conf.lighter_creds is None:
            return self._result(client_order_id, err="order lookup needs signer")
        auth, err = self.signer.create_auth_token_with_expiry()
        if err or not auth:
            return self._result(client_order_id, err="order lookup auth unavailable")
        account = self.conf.lighter_creds.account_index
        params = {"account_index": account, "market_id": self.market_id}
        headers = {"authorization": auth}
        for path, extra in (("accountOrders", {"client_order_indexes": client_order_id}),
                            ("accountActiveOrders", {}),
                            ("accountInactiveOrders", {"limit": 100})):
            cursor = None
            seen = set()
            for _ in range(20):
                query = {**params, **extra}
                if path == "accountOrders":
                    query.pop("market_id")
                if cursor:
                    query["cursor"] = cursor
                try:
                    data = await self._get("/api/v1/" + path, query, headers)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    break  # endpoint may not exist on older deployments
                if not isinstance(data, dict) or data.get("code") != 200 \
                        or not isinstance(data.get("orders"), list):
                    break
                for order in data["orders"]:
                    if not isinstance(order, dict):
                        continue
                    try:
                        matches = int(order.get("client_order_index")) == int(client_order_id)
                        identity = (int(order.get("market_index")) == self.market_id
                                    and int(order.get("owner_account_index")) == account)
                    except (TypeError, ValueError):
                        continue
                    if not matches or not identity:
                        continue
                    info = _terminal_info(order)
                    if info is None:
                        return self._result(client_order_id, status=str(order.get("status", "unknown")))
                    if self.orders_feed:
                        self.orders_feed._resolve(int(client_order_id), info)
                    return self._result(client_order_id, info["status"],
                                        filled_base=info["filled_base"],
                                        avg_px=info["avg_px"], unresolved=False)
                cursor = data.get("next_cursor")
                if path != "accountInactiveOrders" or not isinstance(cursor, str) \
                        or not cursor or cursor in seen:
                    break
                seen.add(cursor)
        return self._result(client_order_id, err="order not conclusively found")

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False,
                         client_order_id: str | None = None) -> dict:
        """Market order with avg-price protection; settle via account ws."""
        assert self.signer is not None
        from lighter import SignerClient
        client_order_id = client_order_id or self.new_client_order_id()
        try:
            coi = int(client_order_id)
            qty = _finite(qty, nonnegative=True)
            limit_px = _finite(limit_px, nonnegative=True)
            base_scaled, price_scaled = qty * 10 ** self.size_decimals, limit_px * 10 ** self.price_decimals
            base_amount, price = round(base_scaled), round(price_scaled)
            if not 0 < coi < (1 << 48) or str(coi) != client_order_id:
                raise ValueError("invalid client order index")
            if not isinstance(is_buy, bool) or not isinstance(reduce_only, bool):
                raise ValueError("invalid order side")
            if qty <= 0 or limit_px <= 0 or qty < self.min_base \
                    or qty * limit_px < self.min_quote:
                raise ValueError("order below market minimum")
            if not math.isclose(base_scaled, base_amount, rel_tol=0, abs_tol=1e-6) \
                    or not math.isclose(price_scaled, price, rel_tol=0, abs_tol=1e-6):
                raise ValueError("quantity or price is off the market grid")
            # The pinned native signer exposes price as ctypes.c_int. Reject
            # values beyond signed int32 rather than relying on native casts.
            if not 0 < base_amount < (1 << 63) or not 0 < price < (1 << 31):
                raise ValueError("order amount outside wire range")
        except (TypeError, ValueError, OverflowError) as e:
            return self._result(client_order_id, "send-failed", err=str(e), unresolved=False)
        fut = self.orders_feed.watch(coi) if self.orders_feed else None
        try:
            _tx, resp, err = await asyncio.wait_for(self.signer.create_order(
                market_index=self.market_id,
                client_order_index=coi,
                base_amount=base_amount,
                price=price,
                is_ask=not is_buy,
                order_type=SignerClient.ORDER_TYPE_MARKET,
                time_in_force=SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=SignerClient.DEFAULT_IOC_EXPIRY,
            ), timeout=self.submit_timeout)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            if getattr(e, "status", None) == 429 or "(429)" in str(e):
                msg = "RATE_LIMITED: " + msg
            return self._result(client_order_id, err=msg)
        code = getattr(resp, "code", None)
        if code is None and isinstance(resp, dict):
            code = resp.get("code")
        # SDK signing/explicit API errors are definite rejections. Missing or
        # 5xx responses may follow acceptance and must remain recoverable.
        if err is not None or (isinstance(code, int) and 400 <= code < 500):
            if fut is not None:
                self.orders_feed.unwatch(coi)
            msg = str(err) if err is not None else \
                f"tx rejected code={code}"
            if "rate limit" in msg.lower():
                msg = "RATE_LIMITED: " + msg
            return self._result(client_order_id, "send-failed", err=msg, unresolved=False)
        if code != 200 or fut is None:
            return self._result(client_order_id, err="submission not conclusively acknowledged")
        try:
            info = await asyncio.wait_for(asyncio.shield(fut), timeout=self.settle_timeout)
            if info["filled_base"] > qty + 1e-12:
                return self._result(client_order_id, err="settlement exceeds requested amount")
            return self._result(client_order_id, info["status"],
                                filled_base=info["filled_base"],
                                avg_px=info.get("avg_px"), unresolved=False)
        except TimeoutError:
            log.warning("[%s] no settle confirmation for coi %d in %.1fs",
                        self.name, coi, self.settle_timeout)
            return self._result(client_order_id, "timeout")

    # -------------------------------------------------------------- accounts

    async def _account(self) -> dict | None:
        c = self.conf.lighter_creds
        if c is None or c.account_index is None:
            return None
        data = await self._get("/api/v1/account",
                               params={"by": "index",
                                       "value": str(c.account_index)})
        if not isinstance(data, dict) or data.get("code") != 200 \
                or not isinstance(data.get("accounts"), list):
            raise RuntimeError(f"[{self.name}] invalid account response")
        for acct in data["accounts"]:
            if isinstance(acct, dict) and acct.get("index") == c.account_index:
                return acct
        raise RuntimeError(f"[{self.name}] requested account not found")

    async def fetch_equity(self):
        acct = await self._account()
        if acct is None:
            return None
        try:
            return (_finite(acct["total_asset_value"]),
                    _finite(acct["available_balance"]))
        except (KeyError, TypeError, ValueError) as e:
            raise RuntimeError(f"[{self.name}] invalid account equity") from e

    async def fetch_position(self) -> float:
        acct = await self._account()
        if acct is None:
            raise RuntimeError(f"[{self.name}] account not found")
        if not isinstance(acct.get("positions"), list):
            raise RuntimeError(f"[{self.name}] invalid account positions")
        found = None
        for p in acct["positions"]:
            if not isinstance(p, dict) or "market_id" not in p:
                raise RuntimeError(f"[{self.name}] invalid position record")
            if int(p["market_id"]) == self.market_id:
                if found is not None:
                    raise RuntimeError(f"[{self.name}] duplicate position record")
                try:
                    sign = int(p["sign"])
                    amount = _finite(p["position"], nonnegative=True)
                    if sign not in (-1, 1) and not (sign == 0 and amount == 0):
                        raise ValueError("invalid position sign")
                    found = sign * amount
                except (KeyError, TypeError, ValueError) as e:
                    raise RuntimeError(f"[{self.name}] invalid position amount") from e
        return found if found is not None else 0.0

    async def close(self) -> None:
        if self.signer is not None:
            try:
                await self.signer.api_client.close()
            except Exception:
                pass
