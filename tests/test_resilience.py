"""Bounded deterministic offline event replay; this is not elapsed-time soak proof."""
import asyncio
import json
import random
from collections import Counter

import pytest

from tests.test_runtime import Exchange, OfflineEngine, fill, runtime_config, until


class Replay:
    def __init__(self):
        self.venues = {}
        self.next_id = 0
        self.ids = set()
        self.events = Counter()
        self.mode = 'normal'
        self.late = {}

    def quiet(self):
        for venue in self.venues.values():
            venue.bids, venue.asks = [(99.9, 20)], [(100.1, 20)]
            self.refresh(venue)

    @staticmethod
    def refresh(venue):
        venue.book.apply_hl([[{'px': p, 'sz': q} for p, q in venue.bids],
                             [{'px': p, 'sz': q} for p, q in venue.asks]])

    def signal(self, buy_entropy):
        self.quiet()
        buy = self.venues['entropy' if buy_entropy else 'hedge']
        sell = self.venues['hedge' if buy_entropy else 'entropy']
        buy.bids, buy.asks = [(99.8, 20)], [(100, 20)]
        sell.bids, sell.asks = [(101, 20)], [(101.2, 20)]
        for venue in self.venues.values():
            self.refresh(venue)

    def release_late(self):
        for venue, oid, terminal in self.late.values():
            venue.resolutions[oid] = terminal
        self.late.clear()


class ReplayExchange(Exchange):
    def __init__(self, conf, replay):
        super().__init__(conf, [(99.9, 20)], [(100.1, 20)])
        self.replay = replay
        self.orders_per_min, self.cap_usd = 100000, 100000
        self.created_tasks = []

    def new_client_order_id(self):
        self.replay.next_id += 1
        oid = f'replay-{self.replay.next_id}'
        assert oid not in self.replay.ids
        self.replay.ids.add(oid)
        return oid

    def start_tasks(self, stop, notify, live):
        tasks = super().start_tasks(stop, notify, live)
        self.created_tasks.extend(tasks)
        return tasks

    async def send_taker(self, **order):
        self.sends.append(order)
        replay = self.replay
        qty = order['qty']
        mode = 'hedge' if order['reduce_only'] else replay.mode
        replay.events[mode] += 1
        if not order['reduce_only']:
            replay.quiet()  # One opportunity per explicit driver event.
        if not order['reduce_only'] and self.key == 'hedge':
            if mode == 'partial':
                qty /= 2
            elif mode == 'reject':
                qty = 0
        terminal = fill(qty, order['limit_px'])
        if qty == 0:
            terminal['status'] = 'canceled'
        self.remote_position += qty * (1 if order['is_buy'] else -1)
        replay.events['filled' if qty else 'unfilled'] += 1
        if mode == 'unknown' and self.key == 'hedge':
            replay.late[order['client_order_id']] = (self, order['client_order_id'], terminal)
            return fill(0, unresolved=True)
        return terminal


def test_seeded_160_cycle_replay_and_pending_restart(tmp_path):
    async def scenario():
        cfg = runtime_config(tmp_path)
        cfg.max_order_notional = 100
        cfg.max_consecutive_errors = 10000
        cfg.position_staleness_sec = 10
        cfg.balance_staleness_sec = 10
        replay = Replay()
        replay.venues = {conf.key: ReplayExchange(conf, replay)
                         for conf in (cfg.entropy, cfg.hedge)}
        rng = random.Random(20260926)
        modes = ['normal', 'partial', 'reject', 'unknown'] * 40
        rng.shuffle(modes)
        # Force a pending outcome at a reproducible interruption boundary.
        modes[79], modes[modes.index('unknown')] = 'unknown', modes[79]
        eng = OfflineEngine(cfg, replay.venues)
        run = asyncio.create_task(eng.run())
        engines = [eng]
        await until(lambda: eng._positions_trusted and all(
            v.free is not None for v in replay.venues.values()))
        try:
            for cycle, mode in enumerate(modes):
                replay.mode = mode
                before = sum(len(v.sends) for v in replay.venues.values())
                replay.signal(buy_entropy=cycle % 2 == 0)
                await until(lambda before=before: sum(
                    len(v.sends) for v in replay.venues.values()) >= before + 2)
                if mode == 'unknown':
                    await until(lambda eng=eng: bool(eng._pending()))
                    count = sum(len(v.sends) for v in replay.venues.values())
                    # Maintain an attractive book while unknown. Opening stays
                    # blocked even after feed updates and multiple recovery polls.
                    replay.signal(buy_entropy=cycle % 2 == 0)
                    await asyncio.sleep(0.015)
                    assert sum(len(v.sends) for v in replay.venues.values()) == count
                    assert eng.recovering and not eng._opening_allowed()
                    replay.quiet()
                    if cycle == 79:
                        run.cancel()  # Simulate interruption after remote acceptance.
                        with pytest.raises(asyncio.CancelledError):
                            await run
                        assert not eng._exec_tasks and not eng._order_tasks
                        assert all(task.done() for task in eng._tasks)
                        assert all(v.closed for v in replay.venues.values())
                        replay.release_late()
                        eng = OfflineEngine(cfg, replay.venues)
                        engines.append(eng)
                        run = asyncio.create_task(eng.run())
                    else:
                        replay.release_late()
                await until(lambda eng=eng: eng.journal is not None and eng._positions_trusted
                            and not eng._pending() and not eng.recovering
                            and not eng._order_tasks and not eng._exec_tasks)
                expected = eng.journal.get_meta('positions')
                for key, venue in replay.venues.items():
                    assert expected[key] == pytest.approx(venue.remote_position, abs=1e-9)
                    assert venue.position == pytest.approx(venue.remote_position, abs=1e-9)
                assert eng._net_ok()
                assert abs(sum(v.remote_position for v in replay.venues.values())) < 0.001
                assert not eng.halted
            sends = [order for venue in replay.venues.values() for order in venue.sends]
            assert len(sends) == len(replay.ids) == len({o['client_order_id'] for o in sends})
            assert len(sends) >= 350
            assert all(replay.events[mode] >= 60 for mode in ('normal', 'partial', 'reject', 'unknown'))
            assert replay.events['hedge'] >= 60
            assert len(engines) == 2
            stats = {'cycles': len(modes), 'orders': len(sends),
                     'filled_orders': replay.events['filled'],
                     'restarts': len(engines) - 1, 'events': dict(replay.events)}
            (tmp_path / 'replay_stats.json').write_text(json.dumps(stats, sort_keys=True))
            print(json.dumps(stats, sort_keys=True))
        finally:
            eng.request_stop()
            await asyncio.wait_for(run, 2)
        assert all(task.done() for engine in engines for task in engine._tasks)
        assert all(task.done() for venue in replay.venues.values() for task in venue.created_tasks)
        assert all(not engine._order_tasks and not engine._exec_tasks for engine in engines)
        assert all(v.closed for v in replay.venues.values())
    asyncio.run(asyncio.wait_for(scenario(), 25))
