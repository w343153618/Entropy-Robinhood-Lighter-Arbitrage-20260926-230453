"""Hyperliquid HIP-3 dex venue adapter (Entropy = dex "io", trade.xyz = "xyz").

Market metadata, account state and order posting use Hyperliquid's public
/info and /exchange REST endpoints via plain aiohttp; the book comes from the
OFFICIAL websocket (see feeds.HLBookFeed). Trading lazily imports the
official `hyperliquid-python-sdk` signing helpers + eth_account —
--record-only data collection needs neither.

IOC limit orders settle synchronously in the /exchange response; unknown
outcomes (timeout/5xx) fall back to orderStatus-by-cloid polling inside
send_taker(), so the engine sees the same unified result shape as the Lighter
venue: {status, filled_base, avg_px, err, unresolved}.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import secrets
import time

import aiohttp

from .book import OrderBook
from .config import VenueConf
from .feeds import HLBookFeed

log = logging.getLogger("hl")

INFO_TIMEOUT = 10.0
HL_TERMINAL_STATUSES = {
    "filled", "canceled", "rejected", "marginCanceled", "vaultWithdrawalCanceled",
    "openInterestCapCanceled", "selfTradeCanceled", "reduceOnlyCanceled",
    "siblingFilledCanceled", "delistedCanceled", "liquidatedCanceled",
    "scheduledCancel", "tickRejected", "minTradeNtlRejected", "perpMarginRejected",
    "reduceOnlyRejected", "badAloPxRejected", "iocCancelRejected",
    "badTriggerPxRejected", "marketOrderNoLiquidityRejected",
    "positionIncreaseAtOpenInterestCapRejected", "positionFlipAtOpenInterestCapRejected",
    "tooAggressiveAtOpenInterestCapRejected", "openInterestIncreaseRejected",
    "insufficientSpotBalanceRejected", "oracleRejected", "perpMaxPositionRejected",
}


def _finite(value, *, nonnegative: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not an amount")
    number = float(value)
    if not math.isfinite(number) or (nonnegative and number < 0):
        raise ValueError("invalid amount")
    return number


class NonceAllocator:
    def __init__(self) -> None:
        self._last = 0

    def next(self) -> int:
        self._last = max(self._last + 1, int(time.time() * 1000))
        return self._last


class HLAccount:
    def __init__(self, private_key: str, account_address: str | None,
                 api_url: str) -> None:
        from eth_account import Account
        self.wallet = Account.from_key(private_key)
        self.query_address = (account_address or self.wallet.address).lower()
        self.is_mainnet = api_url == "https://api.hyperliquid.xyz"
        self.nonces = NonceAllocator()

    def describe(self) -> str:
        s = f"signer={self.wallet.address} account={self.query_address}"
        if self.wallet.address.lower() != self.query_address:
            s += " (agent mode)"
        return s


class HLVenue:
    kind = "hl"

    def __init__(self, conf: VenueConf, api_url: str, ws_url: str,
                 session: aiohttp.ClientSession, settle_timeout_sec: float,
                 submit_timeout_sec: float = 5.0) -> None:
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.api_url = api_url
        self.ws_url = ws_url
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.submit_timeout = submit_timeout_sec
        self.book = OrderBook()
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0     # cumulative filled notional this session
        self.equity = None
        self.free = None
        self.start_equity = None
        self.include_core_equity = True  # cleared when two venues share one account
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.account: HLAccount | None = None
        self.coin = ""
        self.asset_id = -1
        self.size_decimals = 0
        self.min_base = 0.0
        self.min_quote = 10.0
        self._signing = None      # lazy hyperliquid-sdk signing module

    async def _info(self, payload: dict):
        async with self.session.post(
                self.api_url + "/info", json=payload,
                timeout=aiohttp.ClientTimeout(total=INFO_TIMEOUT)) as r:
            r.raise_for_status()
            return await r.json()

    async def load_market(self) -> None:
        dexs = await self._info({"type": "perpDexs"})
        if not isinstance(dexs, list) or any(d is not None and not isinstance(d, dict) for d in dexs):
            raise RuntimeError(f"[{self.name}] invalid dex metadata")
        names = [(d or {}).get("name", "") for d in dexs]
        if self.conf.hl_dex not in names:
            raise RuntimeError(f"[{self.name}] dex '{self.conf.hl_dex}' not "
                               f"found on Hyperliquid (available: "
                               f"{[n for n in names if n][:20]}...)")
        dex_index = names.index(self.conf.hl_dex)
        if dex_index < 1:
            raise RuntimeError(f"[{self.name}] expected a HIP-3 dex")
        meta = await self._info({"type": "meta", "dex": self.conf.hl_dex})
        if not isinstance(meta, dict) or not isinstance(meta.get("universe"), list):
            raise RuntimeError(f"[{self.name}] invalid market metadata")
        want = f"{self.conf.hl_dex}:{self.conf.symbol}"
        for idx, a in enumerate(meta["universe"]):
            if not isinstance(a, dict) or not isinstance(a.get("name"), str):
                raise RuntimeError(f"[{self.name}] invalid market record")
            if a["name"] not in (want, self.conf.symbol):
                continue
            if a.get("isDelisted"):
                raise RuntimeError(f"[{self.name}] {a['name']} is delisted")
            self.coin = a["name"]
            self.asset_id = 110000 + (dex_index - 1) * 10000 + idx
            try:
                self.size_decimals = int(a["szDecimals"])
                if not 0 <= self.size_decimals <= 8:
                    raise ValueError("invalid size decimals")
            except (KeyError, TypeError, ValueError) as e:
                raise RuntimeError(f"[{self.name}] invalid market constraints") from e
            self.min_base = 10 ** -self.size_decimals
            log.info("[%s] %s asset_id=%d szDecimals=%d maxLev=%sx %s",
                     self.name, self.coin, self.asset_id, self.size_decimals,
                     a.get("maxLeverage"),
                     "isolated-only" if a.get("onlyIsolated") else "")
            log.warning("[%s] account/HIP-3 fee unverified; configured fee floor %.4g bps",
                        self.name, self.fee_bps)
            return
        raise RuntimeError(f"[{self.name}] {want} not found")

    def init_signer(self) -> None:
        c = self.conf.hl_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from hyperliquid.utils import signing as hl_signing
        except ImportError as e:
            raise RuntimeError(
                "live trading on Hyperliquid needs the official SDK — "
                "pip install -r requirements-live.txt "
                "(hyperliquid-python-sdk)") from e
        self._signing = hl_signing
        self.account = HLAccount(c.private_key, c.account_address, self.api_url)
        log.info("[%s] %s", self.name, self.account.describe())

    def share_nonces_with(self, other: HLVenue) -> None:
        """One signer address must use one nonce sequence."""
        if (self.account and other.account and
                self.account.wallet.address == other.account.wallet.address):
            other.account.nonces = self.account.nonces
            log.info("[%s]/[%s] same signer — shared nonce allocator",
                     self.name, other.name)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        return [asyncio.create_task(
            HLBookFeed(self.name, self.ws_url, self.coin, self.book,
                       notify).run(stop),
            name=f"book-{self.key}")]

    def ready_to_trade(self) -> bool:
        return self.account is not None

    async def warm_http(self) -> None:
        """Order-path keepalive ping (driven by the engine's keepalive loop)."""
        try:
            await self._info({"type": "exchangeStatus"})
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        if px <= 0:
            return px
        max_dec = max(0, 6 - self.size_decimals)
        sig_dec = 4 - math.floor(math.log10(px))
        dec = max(0, min(max_dec, sig_dec))
        f = 10.0 ** dec
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def new_client_order_id(self) -> str:
        """Random 128-bit IDs are independent of venue and process clocks."""
        return "0x" + secrets.token_hex(16)

    @staticmethod
    def _result(client_order_id: str, status: str = "unknown", *,
                filled_base: float = 0.0, avg_px: float | None = None,
                err: str | None = None, unresolved: bool = True) -> dict:
        return {"client_order_id": client_order_id, "status": status,
                "filled_base": filled_base, "avg_px": avg_px,
                "err": err, "unresolved": unresolved}

    async def resolve_order(self, client_order_id: str) -> dict:
        """Recover by cloid; only terminal identity and matching fills settle."""
        try:
            if not isinstance(client_order_id, str) or len(client_order_id) != 34 \
                    or not client_order_id.startswith("0x"):
                raise ValueError("invalid cloid")
            int(client_order_id, 0)
            return await asyncio.wait_for(self._resolve_order(client_order_id),
                                          timeout=self.settle_timeout)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._result(client_order_id, err=f"order lookup: {type(e).__name__}")

    async def _resolve_order(self, client_order_id: str) -> dict:
        addr = self._query_address()
        if addr is None:
            return self._result(client_order_id, err="order lookup needs account address")
        st = await self._info({"type": "orderStatus", "user": addr, "oid": client_order_id})
        if not isinstance(st, dict) or st.get("status") != "order":
            return self._result(client_order_id)
        wrapper = st.get("order")
        if not isinstance(wrapper, dict):
            return self._result(client_order_id)
        status = wrapper.get("status")
        order = wrapper.get("order")
        if not isinstance(order, dict) or order.get("coin") != self.coin \
                or status not in HL_TERMINAL_STATUSES:
            return self._result(client_order_id)
        if order.get("cloid") not in (None, client_order_id):
            return self._result(client_order_id, err="order identity mismatch")
        original = _finite(order["origSz"], nonnegative=True)
        remaining = _finite(order["sz"], nonnegative=True)
        if original <= 0 or remaining > original or (status == "filled" and remaining != 0):
            return self._result(client_order_id, err="invalid order size")
        expected = original if status == "filled" else original - remaining
        # Explicit placement rejections cannot have fills.
        if status == "rejected" or status.endswith("Rejected"):
            return self._result(client_order_id, status, unresolved=False)
        oid = int(order["oid"])
        start = int(order["timestamp"])
        query = {"type": "userFillsByTime", "user": addr,
                 "startTime": start, "aggregateByTime": False}
        if "statusTimestamp" in wrapper:
            end = int(wrapper["statusTimestamp"])
            if end < start:
                return self._result(client_order_id, err="invalid order timestamps")
            query["endTime"] = end
        fills = await self._info(query)
        # The endpoint caps a response at 2K fills. Missing/truncated history
        # must not masquerade as a zero fill or a complete average price.
        if not isinstance(fills, list) or len(fills) >= 2000:
            return self._result(client_order_id, err="fill history unavailable or truncated")
        qty = quote = 0.0
        seen = set()
        for fill in fills:
            if not isinstance(fill, dict):
                return self._result(client_order_id, err="invalid fill history")
            if fill.get("oid") != oid:
                continue
            if fill.get("coin") != self.coin or fill.get("side") != order.get("side"):
                return self._result(client_order_id, err="fill identity mismatch")
            tid = fill.get("tid")
            if tid is None or tid in seen:
                return self._result(client_order_id, err="invalid fill identity")
            seen.add(tid)
            size = _finite(fill["sz"], nonnegative=True)
            px = _finite(fill["px"], nonnegative=True)
            if size <= 0 or px <= 0:
                return self._result(client_order_id, err="invalid fill amount")
            qty += size
            quote += size * px
        if not math.isclose(qty, expected, rel_tol=0, abs_tol=1e-9) \
                or not math.isfinite(quote):
            return self._result(client_order_id, err="fill history does not match terminal order")
        return self._result(client_order_id, status, filled_base=qty,
                            avg_px=quote / qty if qty else None, unresolved=False)

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False,
                         client_order_id: str | None = None) -> dict:
        assert self.account is not None and self.asset_id >= 0
        client_order_id = client_order_id or self.new_client_order_id()
        try:
            qty = _finite(qty, nonnegative=True)
            limit_px = _finite(limit_px, nonnegative=True)
            if qty <= 0 or limit_px <= 0 or qty < self.min_base \
                    or qty * limit_px < self.min_quote:
                raise ValueError("order below market minimum")
            if not isinstance(is_buy, bool) or not isinstance(reduce_only, bool):
                raise ValueError("invalid order side")
            scaled = qty * 10 ** self.size_decimals
            if not math.isclose(scaled, round(scaled), rel_tol=0, abs_tol=1e-6):
                raise ValueError("quantity is off the market grid")
            if limit_px != self.px_round(limit_px, False):
                raise ValueError("price is off the market grid")
            if not isinstance(client_order_id, str) or len(client_order_id) != 34 \
                    or not client_order_id.startswith("0x"):
                raise ValueError("invalid cloid")
            int(client_order_id, 0)
            from hyperliquid.utils.types import Cloid
            cloid = Cloid.from_str(client_order_id)
            s = self._signing
            order_req = {"coin": self.coin, "is_buy": is_buy, "sz": round(qty, 8),
                         "limit_px": limit_px,
                         "order_type": {"limit": {"tif": "Ioc"}},
                         "reduce_only": reduce_only, "cloid": cloid}
            wire = s.order_request_to_order_wire(order_req, self.asset_id)
            action = s.order_wires_to_order_action([wire])
            nonce = self.account.nonces.next()
            sig = s.sign_l1_action(self.account.wallet, action, None, nonce,
                                   None, self.account.is_mainnet)
            payload = {"action": action, "nonce": nonce, "signature": sig,
                       "vaultAddress": None, "expiresAfter": None}
        except Exception as e:
            return self._result(client_order_id, "send-failed",
                                err=f"local signing/validation failed: {type(e).__name__}",
                                unresolved=False)

        body, err, unresolved = await self._post_exchange(payload)
        if err is not None:
            return self._result(client_order_id, "send-failed", err=err, unresolved=False)
        if not unresolved:
            res = self._parse(body)
            res["client_order_id"] = client_order_id
            if res["filled_base"] > qty + 1e-12:
                res = self._result(client_order_id, err="fill exceeds requested size")
            if not res.get("unresolved"):
                return res
        # One deadline covers every request and sleep, rather than allowing each
        # /info timeout to extend settlement beyond the configured budget.
        deadline = time.monotonic() + self.settle_timeout
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                res = await asyncio.wait_for(self.resolve_order(client_order_id), remaining)
            except TimeoutError:
                break
            if not res["unresolved"]:
                return res
            await asyncio.sleep(min(0.5, max(0, deadline - time.monotonic())))
        return self._result(client_order_id, "timeout")

    async def _post_exchange(self, payload: dict):
        try:
            async with self.session.post(
                    self.api_url + "/exchange", json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.submit_timeout)) as r:
                text = await r.text()
                if r.status == 429:
                    return None, f"RATE_LIMITED: HTTP 429 {text[:150]}", False
                if 400 <= r.status < 500:
                    return None, f"HTTP {r.status}: {text[:250]}", False
                if r.status >= 500:
                    return None, None, True
                return json.loads(text), None, False
        except (TimeoutError, aiohttp.ClientError, json.JSONDecodeError):
            return None, None, True

    @staticmethod
    def _parse(body: dict) -> dict:
        def unknown(msg: str) -> dict:
            return {"status": "unknown", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": True}
        def fail(msg: str) -> dict:
            low = msg.lower()
            if "rate limit" in low or "too many" in low:
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False}
        if not isinstance(body, dict):
            return unknown("malformed exchange response")
        if body.get("status") == "err":
            if not isinstance(body.get("response"), str) or not body["response"]:
                return unknown("invalid rejection response")
            return fail(str(body.get("response")))
        if body.get("status") != "ok":
            return unknown("unexpected exchange response")
        try:
            st = body["response"]["data"]["statuses"][0]
        except (KeyError, IndexError, TypeError):
            return unknown("malformed exchange response")
        if not isinstance(st, dict):
            return unknown("invalid order status")
        if "filled" in st:
            f = st["filled"]
            try:
                qty = _finite(f["totalSz"], nonnegative=True)
                px = _finite(f["avgPx"], nonnegative=True)
                if qty <= 0 or px <= 0:
                    raise ValueError("invalid fill")
                return {"status": "filled", "filled_base": qty, "avg_px": px,
                        "err": None, "unresolved": False}
            except (TypeError, ValueError, KeyError):
                return unknown("invalid filled response")
        if "error" in st:
            if not isinstance(st["error"], str) or not st["error"]:
                return unknown("invalid order rejection")
            msg = str(st["error"])
            if "could not immediately match" in msg.lower():
                return {"status": "canceled", "filled_base": 0.0, "avg_px": None,
                        "err": None, "unresolved": False}
            return fail(msg)
        if "resting" in st:
            return {"status": "resting?", "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True}
        return unknown("unknown order status")

    # -------------------------------------------------------------- accounts

    def _query_address(self):
        if self.account is not None:
            return self.account.query_address
        c = self.conf.hl_creds
        return c.account_address.lower() if c and c.account_address else None

    async def fetch_equity(self):
        """Only collateral usable on the traded dex enters venue risk limits."""
        addr = self._query_address()
        if addr is None:
            return None
        st = await self._info({"type": "clearinghouseState", "user": addr,
                               "dex": self.conf.hl_dex})
        try:
            equity = _finite(st["marginSummary"]["accountValue"])
            free = _finite(st["withdrawable"], nonnegative=True)
            return equity, free
        except (KeyError, TypeError, ValueError) as e:
            raise RuntimeError(f"[{self.name}] invalid clearinghouse equity") from e

    async def fetch_position(self) -> float:
        addr = self._query_address()
        if addr is None:
            raise RuntimeError(f"[{self.name}] missing query address")
        st = await self._info({"type": "clearinghouseState", "user": addr,
                               "dex": self.conf.hl_dex})
        if not isinstance(st, dict) or not isinstance(st.get("assetPositions"), list):
            raise RuntimeError(f"[{self.name}] invalid clearinghouse positions")
        found = None
        for ap in st["assetPositions"]:
            pos = ap.get("position") if isinstance(ap, dict) else None
            if not isinstance(pos, dict) or not isinstance(pos.get("coin"), str):
                raise RuntimeError(f"[{self.name}] invalid position record")
            if pos["coin"] == self.coin:
                if found is not None:
                    raise RuntimeError(f"[{self.name}] duplicate position record")
                try:
                    found = _finite(pos["szi"])
                except (KeyError, TypeError, ValueError) as e:
                    raise RuntimeError(f"[{self.name}] invalid position amount") from e
        return found if found is not None else 0.0

    async def close(self) -> None:
        pass
