import json

import pytest

from tools.healthcheck import check_health


def packet(now=100):
    return {'schema_version': 1, 'ts': now, 'pair_id': 'pair', 'state': 'READY',
                'halt_reason': '', 'recovery_reason': '', 'pending_orders': 0,
                'net_base': 0., 'net_usd': 0., 'books_fresh': {'entropy': True, 'hedge': True},
                'stopped': False, 'opening_allowed': True, 'blocked_reason': ''}


def test_ready_and_record_only_health(tmp_path) -> None:
    path = tmp_path / 'health.json'
    for state in ['READY', 'RECORD_ONLY']:
        data = packet(); data['state'] = state
        path.write_text(json.dumps(data))
        code, _ = check_health(path, now=101)
        assert code == 0


def test_recovering_is_alert_not_restart_instruction(tmp_path) -> None:
    path = tmp_path / 'health.json'
    data = packet(); data.update(state='RECOVERING', pending_orders=1,
                                 recovery_reason='unknown fill')
    path.write_text(json.dumps(data))
    code, message = check_health(path, now=101)
    assert code == 1 and 'RECOVERING' in message and 'restart' not in message.lower()


@pytest.mark.parametrize('change', [{'ts': 80}, {'ts': 110}, {'state': 'HALTED'},
    {'state': 'STOPPED', 'stopped': True}, {'pending_orders': 1},
    {'books_fresh': {'entropy': True, 'hedge': False}}, {'schema_version': 2},
    {'ts': float('nan')}, {'pending_orders': True}, {'net_base': float('inf')},
    {'books_fresh': {'entropy': 'true', 'hedge': True}}])
def test_unhealthy_stale_or_invalid_health_fails_closed(tmp_path, change) -> None:
    path = tmp_path / 'health.json'
    data = packet(); data.update(change)
    path.write_text(json.dumps(data))
    assert check_health(path, now=101)[0] != 0


def test_health_missing_partial_and_pair_mismatch(tmp_path) -> None:
    path = tmp_path / 'health.json'
    assert check_health(path, now=101)[0] != 0
    path.write_text('{')
    assert check_health(path, now=101)[0] != 0
    path.write_text(json.dumps(packet()))
    assert check_health(path, now=101, pair_id='other')[0] != 0


def test_opening_gate_and_recorder_failures_alert(tmp_path) -> None:
    path = tmp_path / 'health.json'
    for change in [{'opening_allowed': False, 'blocked_reason': 'low collateral'},
                   {'recorder': {'healthy': False}}, {'state': []},
                   {'opening_allowed': 'yes'}]:
        data = packet(); data.update(change)
        path.write_text(json.dumps(data))
        assert check_health(path, now=101)[0] != 0


def test_paused_and_missing_opening_gate_are_not_healthy(tmp_path) -> None:
    path = tmp_path / 'health.json'
    data = packet(); data.update(state='PAUSED', opening_allowed=False,
                                 blocked_reason='collateral stale')
    path.write_text(json.dumps(data))
    code, message = check_health(path, now=101)
    assert code == 1 and 'PAUSED' in message
    data = packet(); del data['opening_allowed']
    path.write_text(json.dumps(data))
    assert check_health(path, now=101)[0] != 0
