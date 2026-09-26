"""Offline venue boundary tests: no keys, network or live transactions."""
import asyncio
import sys
from types import SimpleNamespace as NS

from entropy_robinhood_lighter_arbitrage.config import HLCreds, VenueConf
from entropy_robinhood_lighter_arbitrage.venue_hl import HLVenue
from entropy_robinhood_lighter_arbitrage.venue_lighter import (
    AccountOrdersFeed,
    LighterVenue,
)


def hl_venue(session=None, **kwargs):
    conf = VenueConf('entropy', 'hl', 'HL', 'SNDK', 4.5, 100, 120,
                     hl_dex='io', hl_creds=HLCreds(None, '0x' + '1' * 40))
    v = HLVenue(conf, 'https://api.hyperliquid.xyz', 'ws://fake', session, 0.03, **kwargs)
    v.coin = 'io:SNDK'
    v.asset_id = 110000
    v.size_decimals = 2
    v.min_base = 0.01
    return v


def lighter_venue(**kwargs):
    conf = VenueConf('hedge', 'lighter', 'RH', 'SNDK', 0, 100, 30,
                     lighter_profile=NS(api_url='https://fake', name='test'),
                     lighter_creds=NS(account_index=1))
    v = LighterVenue(conf, None, 0.03, **kwargs)
    v.market_id = 32
    v.price_decimals = 2
    v.size_decimals = 2
    v.min_base = 0.01
    return v


def sdk_stub(monkeypatch):
    monkeypatch.setitem(sys.modules, 'lighter', NS(SignerClient=NS(
        ORDER_TYPE_MARKET=1, ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0,
        DEFAULT_IOC_EXPIRY=0)))


def test_lighter_network_failure_retains_late_fill_recovery(monkeypatch):
    sdk_stub(monkeypatch)

    async def scenario():
        v = lighter_venue()
        async def submit(**kwargs):
            raise TimeoutError('reply lost after acceptance')
        v.signer = NS(create_order=submit)
        v.orders_feed = AccountOrdersFeed('RH', 'ws://fake', 32, 1, v.signer)
        r = await v.send_taker(is_buy=False, qty=1, limit_px=100)
        assert r['unresolved'] is True
        coi = r['client_order_id']
        v.orders_feed._handle_orders({'orders': {'32': [{
            'client_order_index': int(coi), 'status': 'filled',
            'filled_base_amount': '1', 'filled_quote_amount': '100'}]}})
        recovered = await v.resolve_order(coi)
        assert recovered['filled_base'] == 1
        assert recovered['avg_px'] == 100 and not recovered['unresolved']

    asyncio.run(scenario())


def test_lighter_submit_deadline_is_independent_from_settlement(monkeypatch):
    sdk_stub(monkeypatch)

    async def scenario():
        v = lighter_venue(submit_timeout_sec=0.01)
        async def submit(**kwargs):
            await asyncio.Event().wait()
        v.signer = NS(create_order=submit)
        r = await asyncio.wait_for(v.send_taker(is_buy=True, qty=1, limit_px=100), 0.2)
        assert r['unresolved'] and r['client_order_id']

    asyncio.run(scenario())


def test_lighter_unknown_or_nonfinite_order_update_never_settles(monkeypatch):
    sdk_stub(monkeypatch)

    async def scenario():
        v = lighter_venue()
        v.orders_feed = AccountOrdersFeed('RH', 'ws://fake', 32, 1, None)
        for status, fb, fq in [('new-status', '1', '100'), ('filled', 'nan', '100'),
                               ('filled', '1', 'inf'), ('filled', '-1', '100'),
                               ('filled', '1', '0')]:
            v.orders_feed._handle_orders({'orders': {'32': [{
                'client_order_index': 9, 'status': status,
                'filled_base_amount': fb, 'filled_quote_amount': fq}]}})
            assert (await v.resolve_order('9'))['unresolved']

    asyncio.run(scenario())


class Reply:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass
    def raise_for_status(self):
        if self.status >= 400: raise RuntimeError('HTTP failure')
    async def json(self): return self.body
    async def text(self):
        import json
        return json.dumps(self.body)


class Http:
    def __init__(self, handler): self.handler = handler
    def get(self, url, **kwargs): return Reply(self.handler(url, kwargs))
    def post(self, url, **kwargs): return Reply(self.handler(url, kwargs))


def test_lighter_restart_recovers_paginated_order_and_never_guesses_absence():
    async def scenario():
        v = lighter_venue()
        v.settle_timeout = 1
        v.signer = NS(create_auth_token_with_expiry=lambda: ('test-token', None))
        def respond(url, kwargs):
            assert kwargs['headers']['authorization'] == 'test-token'
            if url.endswith(('accountOrders', 'accountActiveOrders')):
                return {'code': 200, 'orders': []}
            if 'cursor' not in kwargs['params']:
                return {'code': 200, 'orders': [], 'next_cursor': 'page2'}
            return {'code': 200, 'orders': [{
                'client_order_index': 9, 'market_index': 32, 'owner_account_index': 1,
                'status': 'filled', 'filled_base_amount': '1',
                'filled_quote_amount': '100', 'initial_base_amount': '1'}]}
        v.session = Http(respond)
        r = await v.resolve_order('9')
        assert r['filled_base'] == 1 and not r['unresolved']
        v.session = Http(lambda url, kwargs: {'code': 200, 'orders': []})
        assert (await v.resolve_order('10'))['unresolved']
    asyncio.run(scenario())


def test_lighter_rejects_off_grid_and_nonfinite_quantities_before_submit(monkeypatch):
    sdk_stub(monkeypatch)
    async def scenario():
        v = lighter_venue()
        async def forbidden(**kwargs): raise AssertionError('must not submit')
        v.signer = NS(create_order=forbidden)
        for qty in [float('nan'), float('inf'), -1, 0, 0.015]:
            r = await v.send_taker(is_buy=True, qty=qty, limit_px=100)
            assert not r['unresolved'] and r['err']
        r = await v.send_taker(is_buy=True, qty=1, limit_px=(1 << 31) / 100)
        assert not r['unresolved'] and 'wire range' in r['err']
    asyncio.run(scenario())


def test_lighter_malformed_submit_response_remains_unknown(monkeypatch):
    sdk_stub(monkeypatch)
    async def scenario():
        v = lighter_venue()
        async def submit(**kwargs): return None, None, None
        v.signer = NS(create_order=submit)
        r = await v.send_taker(is_buy=True, qty=1, limit_px=100)
        assert r['unresolved'] and r['client_order_id']
    asyncio.run(scenario())


def test_account_stream_readiness_expires_when_no_frames_arrive():
    import time
    v = lighter_venue()
    v.orders_feed = AccountOrdersFeed('RH', 'ws://fake', 32, 1, None)
    v.orders_feed.ready.set()
    v.orders_feed.alive_ts = time.monotonic() - 1000
    assert not v.ready_to_trade()
    v.orders_feed.alive_ts = time.monotonic()
    assert v.ready_to_trade()


def test_malformed_account_response_is_not_a_flat_position():
    import pytest
    async def scenario():
        v = lighter_venue()
        v.session = Http(lambda url, kwargs: {'code': 200, 'accounts': [{}]})
        with pytest.raises(RuntimeError): await v.fetch_position()
    asyncio.run(scenario())


def test_hl_unique_order_ids_and_restart_resolution_validate_identity():
    async def scenario():
        v = hl_venue()
        ids = {v.new_client_order_id() for _ in range(100)}
        assert len(ids) == 100
        assert all(x.startswith('0x') and len(x) == 34 for x in ids)
        coi = next(iter(ids))
        v.session = Http(lambda url, kwargs: {'status': 'order', 'order': {
            'status': 'filled', 'order': {'coin': 'xyz:SNDK', 'origSz': '1',
                                        'sz': '0', 'oid': 7, 'timestamp': 100}}})
        assert (await v.resolve_order(coi))['unresolved']
    asyncio.run(scenario())


def test_hl_recovers_actual_fill_price_and_does_not_guess_missing_order():
    async def scenario():
        v = hl_venue()
        def respond(url, kwargs):
            if kwargs['json']['type'] == 'orderStatus':
                return {'status': 'order', 'order': {'status': 'filled', 'order': {
                    'coin': 'io:SNDK', 'side': 'B', 'origSz': '1', 'sz': '0',
                    'oid': 7, 'timestamp': 100}}}
            return [{'oid': 7, 'coin': 'io:SNDK', 'side': 'B', 'sz': '1', 'px': '101', 'tid': 5}]
        v.session = Http(respond)
        r = await v.resolve_order('0x' + '1' * 32)
        assert r['filled_base'] == 1 and r['avg_px'] == 101 and not r['unresolved']
        v.session = Http(lambda url, kwargs: {'status': 'unknownOid'})
        assert (await v.resolve_order('0x' + '1' * 32))['unresolved']
    asyncio.run(scenario())


def hl_signer_stub(v, monkeypatch):
    from entropy_robinhood_lighter_arbitrage.venue_hl import NonceAllocator
    monkeypatch.setitem(sys.modules, 'hyperliquid.utils.types', NS(Cloid=NS(
        from_str=lambda value: NS(to_raw=lambda: value))))
    v.account = NS(wallet=None, query_address='0x' + '1' * 40,
                   is_mainnet=True, nonces=NonceAllocator())
    v._signing = NS(order_request_to_order_wire=lambda req, asset: req,
                    order_wires_to_order_action=lambda wires: {'orders': wires},
                    sign_l1_action=lambda *args: {})


def test_hl_submission_returns_journal_id_and_unknown_for_malformed_ack(monkeypatch):
    async def scenario():
        v = hl_venue()
        hl_signer_stub(v, monkeypatch)
        v.session = Http(lambda url, kwargs: {'status': 'ok', 'response': {}}
                         if url.endswith('/exchange') else {'status': 'unknownOid'})
        r = await v.send_taker(is_buy=True, qty=1, limit_px=100,
                               client_order_id='0x' + '2' * 32)
        assert r['unresolved'] and r['client_order_id'] == '0x' + '2' * 32
    asyncio.run(scenario())


def test_hl_equity_is_local_margin_bucket_and_malformed_positions_fail():
    import pytest
    async def scenario():
        v = hl_venue()
        def respond(url, kwargs):
            assert kwargs['json']['type'] == 'clearinghouseState'
            assert kwargs['json']['dex'] == 'io'
            return {'marginSummary': {'accountValue': '100'}, 'withdrawable': '70'}
        v.session = Http(respond)
        assert await v.fetch_equity() == (100, 70)
        with pytest.raises(RuntimeError): await v.fetch_position()
        v.session = Http(lambda url, kwargs: {'assetPositions': []})
        assert await v.fetch_position() == 0
    asyncio.run(scenario())


def test_malformed_market_metadata_fails_before_enabling_trading():
    import pytest
    async def scenario():
        lv = lighter_venue()
        lv.session = Http(lambda url, kwargs: {'code': 200, 'order_books': [{
            'symbol': 'SNDK', 'status': 'active', 'market_id': 32,
            'supported_price_decimals': -1, 'supported_size_decimals': 2,
            'min_base_amount': '0.01', 'min_quote_amount': '10'}]})
        with pytest.raises(RuntimeError): await lv.load_market()
        hv = hl_venue()
        hv.session = Http(lambda url, kwargs: [None, {'name': 'io'}]
                          if kwargs['json']['type'] == 'perpDexs' else
                          {'universe': [{'name': 'io:SNDK', 'szDecimals': -1}]})
        with pytest.raises(RuntimeError): await hv.load_market()
    asyncio.run(scenario())


def test_lighter_public_fee_floor_never_reduces_configured_fee():
    async def scenario():
        v = lighter_venue()
        v.fee_bps = 3
        v.session = Http(lambda url, kwargs: {'code': 200, 'order_books': [{
            'symbol': 'SNDK', 'status': 'active', 'market_id': 32, 'market_type': 'perp',
            'supported_price_decimals': 2, 'supported_size_decimals': 2,
            'min_base_amount': '0.01', 'min_quote_amount': '10',
            'taker_fee': '0.05', 'multiplier': '1'}]})
        await v.load_market()
        assert v.fee_bps >= 5
        v.fee_bps = 10
        await v.load_market()
        assert v.fee_bps == 10
    asyncio.run(scenario())


def test_lighter_cancellation_preserves_order_identity_for_recovery(monkeypatch):
    sdk_stub(monkeypatch)
    import pytest
    async def scenario():
        v = lighter_venue()
        accepted = asyncio.Event()
        async def submit(**kwargs):
            accepted.set()
            await asyncio.Event().wait()
        v.signer = NS(create_order=submit)
        v.orders_feed = AccountOrdersFeed('RH', 'ws://fake', 32, 1, v.signer)
        task = asyncio.create_task(v.send_taker(is_buy=True, qty=1, limit_px=100,
                                                client_order_id='9'))
        await accepted.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        v.orders_feed._handle_orders({'orders': {'32': [{
            'client_order_index': 9, 'status': 'filled',
            'filled_base_amount': '1', 'filled_quote_amount': '100'}]}})
        assert (await v.resolve_order('9'))['filled_base'] == 1
    asyncio.run(scenario())


def test_hl_recovery_requires_complete_matching_fill_history():
    async def scenario():
        v = hl_venue()
        def respond(url, kwargs):
            if kwargs['json']['type'] == 'orderStatus':
                return {'status': 'order', 'order': {'status': 'filled', 'order': {
                    'coin': 'io:SNDK', 'side': 'B', 'origSz': '1', 'sz': '0',
                    'oid': 7, 'timestamp': 100}}}
            return [{'oid': 7, 'coin': 'io:SNDK', 'side': 'B', 'sz': '0.5', 'px': '101', 'tid': 5}]
        v.session = Http(respond)
        assert (await v.resolve_order('0x' + '1' * 32))['unresolved']
    asyncio.run(scenario())


def test_lighter_client_order_ids_do_not_repeat_across_instances():
    a, b = lighter_venue(), lighter_venue()
    ids = [a.new_client_order_id(), b.new_client_order_id(), a.new_client_order_id()]
    assert len(set(ids)) == 3
    assert all(0 < int(x) < 2 ** 48 for x in ids)


def test_hl_recovery_bounds_fill_query_to_terminal_timestamp():
    async def scenario():
        v = hl_venue()
        def respond(url, kwargs):
            query = kwargs['json']
            if query['type'] == 'orderStatus':
                return {'status': 'order', 'order': {'status': 'filled',
                    'statusTimestamp': 105, 'order': {'coin': 'io:SNDK', 'side': 'B',
                    'origSz': '1', 'sz': '0', 'oid': 7, 'timestamp': 100}}}
            assert query['startTime'] == 100 and query['endTime'] == 105
            return [{'oid': 7, 'coin': 'io:SNDK', 'side': 'B', 'sz': '1', 'px': '101', 'tid': 5}]
        v.session = Http(respond)
        assert not (await v.resolve_order('0x' + '1' * 32))['unresolved']
    asyncio.run(scenario())


def test_lighter_zero_position_sign_is_flat_but_duplicates_are_invalid():
    import pytest
    async def scenario():
        v = lighter_venue()
        positions = [{'market_id': 32, 'sign': 0, 'position': '0'}]
        v.session = Http(lambda url, kwargs: {'code': 200, 'accounts': [
            {'index': 1, 'positions': positions}]})
        assert await v.fetch_position() == 0
        positions.append({'market_id': 32, 'sign': 1, 'position': '1'})
        with pytest.raises(RuntimeError): await v.fetch_position()
    asyncio.run(scenario())
