"""Hyperliquid /exchange response parsing + Lighter account-orders settle
resolution (the two places where a malformed shape used to kill live order
paths)."""
from __future__ import annotations

from entropy_robinhood_lighter_arbitrage.venue_hl import HLVenue
from entropy_robinhood_lighter_arbitrage.venue_lighter import AccountOrdersFeed


def test_hl_parse_filled() -> None:
    r = HLVenue._parse({
        "status": "ok",
        "response": {"type": "order", "data": {"statuses": [
            {"filled": {"totalSz": "2.5", "avgPx": "100.1"}}]}},
    })
    assert r["status"] == "filled"
    assert r["filled_base"] == 2.5
    assert r["avg_px"] == 100.1
    assert r["err"] is None and not r["unresolved"]


def test_hl_parse_error() -> None:
    r = HLVenue._parse({
        "status": "err",
        "response": "Order would immediately match against your own order",
    })
    assert r["status"] == "send-failed"
    assert r["err"] is not None
    assert not r["unresolved"]


def test_hl_parse_rate_limited() -> None:
    r = HLVenue._parse({"status": "err",
                        "response": "rate limit exceeded"})
    assert r["err"].startswith("RATE_LIMITED")


def test_hl_parse_resting_unresolved() -> None:
    r = HLVenue._parse({
        "status": "ok",
        "response": {"type": "order", "data": {"statuses": [
            {"resting": {"oid": 1}}]}},
    })
    assert r["unresolved"] is True


def test_hl_parse_malformed() -> None:
    r = HLVenue._parse({"status": "ok", "response": {}})
    assert r["status"] == "unknown"
    assert r["err"] is not None and r["unresolved"]


def test_account_orders_feed_resolution() -> None:
    import asyncio

    async def scenario() -> None:
        feed = AccountOrdersFeed("RH", "ws://x", 32, 1, signer=None)
        # open status is ignored, terminal status resolves the watch future
        feed._handle_orders({"orders": {"32": [{"client_order_index": 7,
                                               "status": "in-progress"}]}})
        fut = feed.watch(7)
        assert not fut.done()
        feed._handle_orders({"orders": {"32": [
            {"client_order_index": 7, "status": "filled",
             "filled_base_amount": "1.5", "filled_quote_amount": "150.0"}]}})
        info = await asyncio.wait_for(fut, timeout=1.0)
        assert info["status"] == "filled"
        assert info["filled_base"] == 1.5
        assert info["avg_px"] == 100.0

    asyncio.run(scenario())


def test_account_orders_feed_terminal_cache() -> None:
    import asyncio

    async def scenario() -> None:
        feed = AccountOrdersFeed("RH", "ws://x", 32, 1, signer=None)
        feed._handle_orders({"orders": {"32": [
            {"client_order_index": 9, "status": "canceled",
             "filled_base_amount": "0", "filled_quote_amount": "0"}]}})
        fut = feed.watch(9)  # already terminal -> resolves immediately
        info = await asyncio.wait_for(fut, timeout=1.0)
        assert info["status"] == "canceled"
        assert info["filled_base"] == 0.0

    asyncio.run(scenario())


def test_hl_parse_invalid_rejection_and_fill_are_unknown() -> None:
    for body in [None, {'status': 'err', 'response': None},
                 {'status': 'ok', 'response': {'data': {'statuses': [
                     {'filled': {'totalSz': 'nan', 'avgPx': '100'}}]}}},
                 {'status': 'ok', 'response': {'data': {'statuses': [
                     {'error': None}]}}}]:
        assert HLVenue._parse(body)['unresolved']
