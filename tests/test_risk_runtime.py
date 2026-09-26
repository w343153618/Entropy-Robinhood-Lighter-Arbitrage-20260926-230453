"""Risk gates use the same lifecycle/journal as execution, with no network IO."""
import asyncio
import json
import time

import pytest

from entropy_robinhood_lighter_arbitrage.engine import Engine
from entropy_robinhood_lighter_arbitrage.journal import JournalError, OrderJournal
from tests.test_runtime import OfflineEngine, exchanges, fill, runtime_config, until
from tools.healthcheck import check_health


async def ready_engine(tmp_path, **config):
    cfg = runtime_config(tmp_path, **config)
    vs = exchanges(cfg)
    eng = OfflineEngine(cfg, vs)
    eng.venues = vs
    eng.entropy, eng.hedge = vs.values()
    eng._open_journal()
    await eng._reconcile_positions(hedge=False)
    await eng._poll_balances()
    assert eng._opening_allowed()
    return eng, vs


def test_low_collateral_and_failed_balance_read_are_not_healthy(tmp_path):
    async def scenario():
        eng, vs = await ready_engine(tmp_path)
        try:
            vs['entropy'].free = 0
            eng._write_health()
            data = json.loads((tmp_path / 'health.json').read_text())
            assert data['state'] == 'PAUSED' and not data['opening_allowed']
            assert check_health(tmp_path / 'health.json')[0] == 1
            async def unavailable():
                raise OSError('transport unavailable')
            vs['entropy'].fetch_equity = unavailable
            vs['entropy'].free = 1000
            await eng._poll_balances()
            assert 'entropy' not in eng._balance_checked
            assert not eng._opening_allowed()
        finally:
            eng.journal.close()
    asyncio.run(scenario())


def test_order_budget_reserves_recovery_capacity_and_uses_available_venue(tmp_path):
    async def scenario():
        eng, vs = await ready_engine(tmp_path)
        try:
            a, b = vs.values()
            a.orders_per_min = 3
            eng._record_send(a)
            assert not eng._opening_allowed()
            assert eng._venue_rate_ok(a)  # Two sends still reserved for recovery.
            a.position = b.position = 1
            eng._venue_down[a.key] = time.time()
            selected = eng._hedge_candidate(2, executable=True)
            assert selected and selected[0] is b
        finally:
            eng.request_stop()
            if eng._poke_handle:
                eng._poke_handle.cancel()
            eng.journal.close()
    asyncio.run(scenario())


def test_low_disk_halts_persistently_before_order_submission(tmp_path, monkeypatch):
    async def scenario():
        eng, vs = await ready_engine(tmp_path)
        try:
            eng.cfg.min_disk_free_mb = eng._disk_free_mb + 1
            assert not eng._opening_allowed()
            assert eng.halted and 'disk' in eng.halt_reason
            assert all(not v.sends for v in vs.values())
            assert eng.journal.get_meta('halt_reason') == eng.halt_reason
        finally:
            eng.journal.close()
    asyncio.run(scenario())


def test_journal_prepare_failure_never_sends_a_leg(tmp_path, monkeypatch):
    def fail_prepare(self, intents):
        raise JournalError('simulated disk write failure')
    monkeypatch.setattr(OrderJournal, 'prepare_batch', fail_prepare)
    async def scenario():
        cfg = runtime_config(tmp_path, http_keepalive_sec=0)
        vs = exchanges(cfg)
        eng = OfflineEngine(cfg, vs)
        run = asyncio.create_task(eng.run())
        try:
            await until(lambda: eng.halted)
            assert all(not v.sends for v in vs.values())
            assert not eng._pending()
        finally:
            eng.request_stop()
            await run
    asyncio.run(scenario())


def test_late_terminal_requires_authoritative_read_after_grace(tmp_path):
    async def scenario():
        eng, vs = await ready_engine(tmp_path, reconcile_grace_sec=5)
        try:
            intents = [eng._intent(v, key == 'entropy', 1, 100, False, 'pair')
                       for key, v in vs.items()]
            eng.journal.prepare_batch(intents)
            for intent, v in zip(intents, vs.values()):
                eng.journal.mark_submitted(intent['id'])
                eng._accept_result(v, intent, fill(0, unresolved=True))
                v.resolutions[intent['id']] = fill(1)
                v.remote_position = 1 if intent['is_buy'] else -1
            await eng._reconcile_positions(hedge=False)
            assert not eng._pending()
            eng._clear_recovery()
            assert eng.recovering and not eng._positions_trusted
            assert not eng._opening_allowed()
            eng._last_order_mono.clear()  # Advance only the settlement grace.
            await eng._reconcile_positions(hedge=False)
            assert eng._positions_trusted and not eng.recovering
            assert all(not v.sends for v in vs.values())
        finally:
            eng.journal.close()
    asyncio.run(scenario())


def test_drawdown_and_recovery_timeout_require_operator_acknowledgement(tmp_path):
    async def scenario():
        eng, vs = await ready_engine(tmp_path)
        try:
            async def lower_equity():
                return 950, 950
            for v in vs.values():
                v.fetch_equity = lower_equity
            await eng._poll_balances()
            assert eng.halted and 'drawdown' in eng.halt_reason
            saved = eng.journal.get_meta('halt_reason')
            eng._clear_recovery()
            assert eng.halted and eng.journal.get_meta('halt_reason') == saved
            eng.journal.acknowledge_halt()
            eng.halted = False
            eng._enter_recovery('unknown')
            eng._recovery_since -= eng.cfg.recovery_timeout_sec + 1
            eng._check_recovery_deadline()
            assert eng.halted and 'deadline' in eng.journal.get_meta('halt_reason')
        finally:
            eng.journal.close()
    asyncio.run(scenario())


def test_background_task_failure_stops_and_preserves_halt(tmp_path):
    async def scenario():
        cfg = runtime_config(tmp_path)
        vs = exchanges(cfg)
        async def broken_feed():
            raise RuntimeError('simulated critical task failure')
        vs['entropy'].start_tasks = lambda *args: [asyncio.create_task(broken_feed(), name='broken')]
        eng = OfflineEngine(cfg, vs)
        with pytest.raises(RuntimeError, match='background task broken'):
            await asyncio.wait_for(eng.run(), timeout=1)
        assert eng.halted and not eng._order_tasks
        health = json.loads((tmp_path / 'health.json').read_text())
        assert health['state'] == 'STOPPED' and health['halt_reason']
    asyncio.run(scenario())


@pytest.mark.parametrize('values', [
    {'filled_base': True}, {'filled_base': float('nan')}, {'filled_base': 2},
    {'filled_base': -1}, {'avg_px': False}, {'avg_px': float('inf')},
    {'unresolved': 'false'}, {'unresolved': None},
])
def test_malformed_result_never_becomes_definitive_no_fill(values):
    result = Engine._validated_result({'id': 'one', 'qty': 1}, {**fill(1), **values})
    assert result['unresolved'] and result['status'] == 'invalid-response'


def test_numeric_result_fields_are_normalized():
    result = Engine._validated_result({'id': 'one', 'qty': 1},
                                      {**fill(1), 'filled_base': '1', 'avg_px': '100'})
    assert result['avg_px'] == 100.0 and result['filled_base'] == 1.0


def test_future_journal_schema_is_not_silently_downgraded(tmp_path):
    path = str(tmp_path / 'orders.sqlite3')
    j = OrderJournal(path, 'pair')
    j.set_meta('schema_version', 2)
    j.close()
    with pytest.raises(JournalError, match='unsupported'):
        OrderJournal(path, 'pair')


@pytest.mark.parametrize('field,value', [
    ('positions', {'entropy': True, 'hedge': 0}),
    ('positions', {'entropy': 0}),
    ('equity_high_water', '1000'),
])
def test_semantically_invalid_persistent_state_never_opens_orders(tmp_path, field, value):
    async def scenario():
        eng, vs = await ready_engine(tmp_path)
        eng.journal.set_meta(field, value)
        eng.journal.close()
        restarted = OfflineEngine(eng.cfg, vs)
        with pytest.raises(JournalError, match='invalid persistent'):
            await restarted.run()
        assert all(not v.sends for v in vs.values())
    asyncio.run(scenario())
