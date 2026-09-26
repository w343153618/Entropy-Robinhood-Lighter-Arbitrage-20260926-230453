"""Feed protocol parsing (entropy_robinhood_lighter_arbitrage.feeds): channel id extraction and
Lighter order-book frame routing."""
from __future__ import annotations

from entropy_robinhood_lighter_arbitrage.book import OrderBook
from entropy_robinhood_lighter_arbitrage.feeds import LighterBookFeed, _chan_id


def test_chan_id_separators() -> None:
    assert _chan_id("order_book/32") == 32
    assert _chan_id("order_book:32") == 32
    assert _chan_id("order_book") is None
    assert _chan_id("order_book/abc") is None


class FakeWS:
    def __init__(self) -> None:
        self.sent = []

    async def send(self, raw: str) -> None:
        self.sent.append(raw)


def test_lighter_snapshot_fills_book() -> None:
    import asyncio

    async def scenario() -> None:
        book = OrderBook()
        notified = []
        feed = LighterBookFeed("RH", "ws://x", 32, book, lambda: notified.append(1))
        ws = FakeWS()
        await feed._handle_book(ws, {
            "type": "subscribed/order_book",
            "channel": "order_book/32",
            "order_book": {"bids": [{"price": "100.0", "size": "5"}],
                           "asks": [{"price": "100.1", "size": "4"}],
                           "nonce": 10},
        }, snapshot=True)
        assert feed._synced is True
        assert feed._nonce == 10
        assert book.best_bid() == 100.0 and book.best_ask() == 100.1
        assert notified

    asyncio.run(scenario())


def test_lighter_diff_applies() -> None:
    import asyncio

    async def scenario() -> None:
        book = OrderBook()
        feed = LighterBookFeed("RH", "ws://x", 32, book, lambda: None)
        ws = FakeWS()
        await feed._handle_book(ws, {
            "type": "subscribed/order_book", "channel": "order_book/32",
            "order_book": {"bids": [{"price": "100.0", "size": "5"}],
                           "asks": [{"price": "100.1", "size": "4"}],
                           "nonce": 10}},
            snapshot=True)
        await feed._handle_book(ws, {
            "type": "update/order_book", "channel": "order_book/32",
            "order_book": {"bids": [{"price": "100.0", "size": "3"}],
                           "asks": [{"price": "100.05", "size": "4"}],
                           "begin_nonce": 10, "nonce": 11}},
            snapshot=False)
        assert book.best_bid() == 100.0
        assert book.bids[100.0] == 3
        assert book.best_ask() == 100.05
        assert feed._nonce == 11

    asyncio.run(scenario())


def test_lighter_nonce_gap_resubscribes() -> None:
    import asyncio

    async def scenario() -> None:
        book = OrderBook()
        feed = LighterBookFeed("RH", "ws://x", 32, book, lambda: None)
        ws = FakeWS()
        await feed._handle_book(ws, {
            "type": "subscribed/order_book", "channel": "order_book/32",
            "order_book": {"bids": [{"price": "100.0", "size": "5"}],
                           "asks": [{"price": "100.1", "size": "4"}],
                           "nonce": 10}},
            snapshot=True)
        # gap: begin_nonce 12 while we held 10 -> resubscribe, clear book
        await feed._handle_book(ws, {
            "type": "update/order_book", "channel": "order_book/32",
            "order_book": {"bids": [{"price": "99.0", "size": "5"}],
                           "asks": [{"price": "99.1", "size": "4"}],
                           "begin_nonce": 12, "nonce": 13}},
            snapshot=False)
        assert feed._synced is False
        assert book.bids == {} and book.asks == {}
        # unsubscribe + resubscribe were sent on the wire
        assert len(ws.sent) == 2
        assert "unsubscribe" in ws.sent[0] and "subscribe" in ws.sent[1]

    asyncio.run(scenario())


def test_wrong_market_channel_ignored() -> None:
    import asyncio

    async def scenario() -> None:
        book = OrderBook()
        feed = LighterBookFeed("RH", "ws://x", 32, book, lambda: None)
        ws = FakeWS()
        # a different market's snapshot must not touch our book
        await feed._handle_book(ws, {
            "type": "subscribed/order_book", "channel": "order_book/31",
            "order_book": {"bids": [{"price": "1.0", "size": "9"}],
                           "asks": [{"price": "1.1", "size": "9"}],
                           "nonce": 1}},
            snapshot=True)
        assert book.bids == {} and book.asks == {}
        assert feed._synced is False

    asyncio.run(scenario())


def test_lighter_strict_continuity_missing_and_duplicate() -> None:
    import asyncio

    async def scenario():
        for begin, end in [(11, 12), (None, 12), (10, None), (9, 12)]:
            book = OrderBook()
            feed = LighterBookFeed('RH', 'ws://x', 32, book, lambda: None)
            ws = FakeWS()
            await feed._handle_book(ws, {'channel': 'order_book/32', 'order_book': {
                'bids': [{'price': '100', 'size': '2'}],
                'asks': [{'price': '101', 'size': '2'}], 'nonce': 10}}, True)
            await feed._handle_book(ws, {'channel': 'order_book/32', 'order_book': {
                'bids': [{'price': '100', 'size': '7'}],
                'begin_nonce': begin, 'nonce': end}}, False)
            assert not feed._synced and not book.ready
            assert len(ws.sent) == 2
        book = OrderBook()
        feed = LighterBookFeed('RH', 'ws://x', 32, book, lambda: None)
        ws = FakeWS()
        await feed._handle_book(ws, {'channel': 'order_book/32', 'order_book': {
            'bids': [{'price': '100', 'size': '2'}],
            'asks': [{'price': '101', 'size': '2'}], 'nonce': 10}}, True)
        # An already-applied interval cannot overwrite newer depth.
        await feed._handle_book(ws, {'channel': 'order_book/32', 'order_book': {
            'bids': [{'price': '100', 'size': '99'}],
            'begin_nonce': 8, 'nonce': 10}}, False)
        assert book.bids[100] == 2 and feed._nonce == 10
    asyncio.run(scenario())


def test_hl_old_server_timestamp_and_pong_do_not_refresh_market() -> None:
    import time

    from entropy_robinhood_lighter_arbitrage.feeds import HLBookFeed
    book = OrderBook()
    feed = HLBookFeed('HL', 'ws://x', 'io:SNDK', book, lambda: None)
    feed._on_frame({'channel': 'l2Book', 'data': {'coin': 'io:SNDK',
        'time': (time.time() - 100) * 1000,
        'levels': [[{'px': '100', 'sz': '1'}], [{'px': '101', 'sz': '1'}]]}})
    feed._on_frame({'channel': 'pong'})
    assert not book.is_fresh(10)


def test_lighter_silent_market_expires_despite_pings() -> None:
    import time
    book = OrderBook()
    LighterBookFeed('RH', 'ws://x', 32, book, lambda: None)
    book.apply_lighter({'bids': [{'price': '100', 'size': '1'}],
                        'asks': [{'price': '101', 'size': '1'}]}, True)
    book.last_update_ts = time.time() - 61
    book.touch()
    assert not book.is_fresh(10)


def test_periodic_snapshot_refresh_clears_stale_authority() -> None:
    import asyncio
    async def scenario():
        book = OrderBook()
        feed = LighterBookFeed('RH', 'ws://x', 32, book, lambda: None,
                               snapshot_refresh_sec=.01)
        book.apply_lighter({'bids': [{'price': '100', 'size': '1'}],
                            'asks': [{'price': '101', 'size': '1'}]}, True)
        feed._nonce, feed._synced = 10, True
        ws = FakeWS()
        task = asyncio.create_task(feed._refresh_loop(ws))
        try:
            async def refreshed():
                while len(ws.sent) < 2:
                    await asyncio.sleep(.001)
            await asyncio.wait_for(refreshed(), .2)
            assert not book.ready and not feed._synced and feed._nonce is None
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_feed_cancellation_clears_book_and_drains_helpers(monkeypatch) -> None:
    import asyncio
    import json
    import time

    from entropy_robinhood_lighter_arbitrage import feeds

    async def scenario(kind):
        book = OrderBook()
        notified = asyncio.Event()
        stop = asyncio.Event()
        class Socket(FakeWS):
            def __init__(self):
                super().__init__()
                self.delivered = False
                self.closed = asyncio.Event()
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            def __aiter__(self):
                return self
            async def __anext__(self):
                if self.delivered:
                    await self.closed.wait()
                    raise StopAsyncIteration
                self.delivered = True
                if kind == 'hl':
                    return json.dumps({'channel': 'l2Book', 'data': {'coin': 'io:SNDK',
                        'time': time.time() * 1000,
                        'levels': [[{'px': '100', 'sz': '1'}],
                                   [{'px': '101', 'sz': '1'}]]}})
                return json.dumps({'type': 'subscribed/order_book',
                    'channel': 'order_book/32', 'order_book': {'nonce': 10,
                    'bids': [{'price': '100', 'size': '1'}],
                    'asks': [{'price': '101', 'size': '1'}]}})
            async def close(self):
                self.closed.set()
        socket = Socket()
        monkeypatch.setattr(feeds, 'ws_connect', lambda *args, **kwargs: socket)
        feed = (feeds.HLBookFeed('HL', 'ws://x', 'io:SNDK', book, notified.set)
                if kind == 'hl' else
                feeds.LighterBookFeed('RH', 'ws://x', 32, book, notified.set))
        task = asyncio.create_task(feed.run(stop))
        await asyncio.wait_for(notified.wait(), .2)
        assert book.ready
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert not book.ready
        assert len(asyncio.all_tasks()) == 1
    asyncio.run(scenario('hl'))
    asyncio.run(scenario('lighter'))


def test_lighter_invalid_diff_rebuilds_and_missing_snapshot_nonce_fails_closed() -> None:
    import asyncio
    async def scenario():
        for bad in [float('nan'), float('inf'), -1]:
            book = OrderBook()
            feed = LighterBookFeed('RH', 'ws://x', 32, book, lambda: None)
            ws = FakeWS()
            await feed._handle_book(ws, {'channel': 'order_book/32', 'order_book': {
                'nonce': 10, 'bids': [{'price': '100', 'size': '1'}],
                'asks': [{'price': '101', 'size': '1'}]}}, True)
            await feed._handle_book(ws, {'channel': 'order_book/32', 'order_book': {
                'begin_nonce': 10, 'nonce': 11,
                'bids': [{'price': bad, 'size': '1'}]}}, False)
            assert not book.ready and not feed._synced
            assert len(ws.sent) == 2
        book = OrderBook()
        feed = LighterBookFeed('RH', 'ws://x', 32, book, lambda: None)
        await feed._handle_book(FakeWS(), {'channel': 'order_book/32', 'order_book': {
            'bids': [{'price': '100', 'size': '1'}],
            'asks': [{'price': '101', 'size': '1'}]}}, True)
        assert not feed._synced and not book.ready
    asyncio.run(scenario())


def test_hl_unsubscribe_clears_ready() -> None:
    from entropy_robinhood_lighter_arbitrage.feeds import HLBookFeed
    book = OrderBook()
    book.apply_hl([[{'px': 100, 'sz': 1}], [{'px': 101, 'sz': 1}]])
    feed = HLBookFeed('HL', 'ws://x', 'io:SNDK', book, lambda: None)
    feed._on_frame({'channel': 'subscriptionResponse', 'data': {
        'method': 'unsubscribe', 'subscription': {'coin': 'io:SNDK', 'type': 'l2Book'}}})
    assert not book.ready


def test_parse_error_clears_market_and_is_observable(monkeypatch) -> None:
    import asyncio

    from entropy_robinhood_lighter_arbitrage import feeds

    async def scenario(kind):
        book = OrderBook()
        stop = asyncio.Event()
        class BadSocket(FakeWS):
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            def __aiter__(self):
                return self
            async def __anext__(self):
                stop.set()
                return '{not-json'
            async def close(self):
                pass
        monkeypatch.setattr(feeds, 'ws_connect', lambda *args, **kwargs: BadSocket())
        feed = (feeds.HLBookFeed('HL', 'ws://x', 'io:SNDK', book, lambda: None)
                if kind == 'hl' else
                feeds.LighterBookFeed('RH', 'ws://x', 32, book, lambda: None))
        book.apply_hl([[{'px': 100, 'sz': 1}], [{'px': 101, 'sz': 1}]])
        await asyncio.wait_for(feed.run(stop), .2)
        assert not book.ready and feed.last_error is not None
        assert len(asyncio.all_tasks()) == 1
    asyncio.run(scenario('hl'))
    asyncio.run(scenario('lighter'))
