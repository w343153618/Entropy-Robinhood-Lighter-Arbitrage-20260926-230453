"""Offline safety and packaging contract for the command line."""
import logging
from logging.handlers import RotatingFileHandler
from types import SimpleNamespace

import pytest

from entropy_robinhood_lighter_arbitrage import main as cli


def test_mode_must_be_explicit_before_config_read(monkeypatch) -> None:
    monkeypatch.setattr(cli, 'load_config', lambda *a, **kw: pytest.fail('config must not load'))
    with pytest.raises(SystemExit) as caught:
        cli.main(['--symbol', 'SNDK', '--hedge', 'lighter-rh'])
    assert caught.value.code == 2


def test_modes_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        cli.main(['--symbol', 'SNDK', '--hedge', 'lighter-rh', '--live', '--record-only'])


def test_check_config_does_not_run_engine_or_read_credentials(monkeypatch, capsys) -> None:
    calls = []
    def config(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(pair_id='pair', state_db='state/pair/orders.sqlite3',
                               health_file='logs/pair/health.json')
    monkeypatch.setattr(cli, 'load_config', config)
    monkeypatch.setattr(cli, 'amain', lambda *args, **kwargs: pytest.fail('no runtime'))
    assert cli.main(['--symbol', 'SNDK', '--hedge', 'lighter-rh', '--check-config']) == 0
    assert calls[0]['read_credentials'] is False
    assert 'config valid' in capsys.readouterr().out


def test_plain_logging_keeps_rotating_file_and_console(tmp_path) -> None:
    path = tmp_path / 'engine.log'
    cli.setup_logging('INFO', str(path), max_bytes=128, backup_count=2, console=True)
    managed = [h for h in logging.getLogger().handlers if getattr(h, '_arb_managed', False)]
    assert any(isinstance(h, RotatingFileHandler) for h in managed)
    assert any(type(h) is logging.StreamHandler for h in managed)
    for _ in range(8):
        logging.getLogger('cli-test').info('bounded disk log ' * 10)
    cli.close_logging()
    assert path.exists() and path.with_name('engine.log.1').exists()
    assert len(list(tmp_path.iterdir())) <= 3


def test_dashboard_hurdles_follow_actual_buy_sell_directions() -> None:
    from entropy_robinhood_lighter_arbitrage.book import OrderBook
    from entropy_robinhood_lighter_arbitrage.dashboard import Dashboard
    calls = []
    entropy = SimpleNamespace(book=OrderBook(), fee_bps=0)
    hedge = SimpleNamespace(book=OrderBook(), fee_bps=0)
    def threshold(buy, sell):
        calls.append((buy, sell))
        return 4
    eng = SimpleNamespace(entropy=entropy, hedge=hedge, _eff_threshold=threshold,
                          _armed={}, premium_bps=lambda: 0)
    Dashboard(eng, None, None)._premium_panel()
    assert calls == [(hedge, entropy), (entropy, hedge)]


def test_runtime_exception_is_logged_and_returns_failure(monkeypatch, tmp_path) -> None:
    cfg = SimpleNamespace(creds_complete=True, dashboard=False, log_level='INFO',
                          log_file=str(tmp_path / 'engine.log'), log_max_bytes=1024,
                          log_backup_count=2)
    monkeypatch.setattr(cli, 'load_config', lambda *args, **kwargs: cfg)
    async def failed(*args, **kwargs):
        raise TimeoutError('offline fake transport failure')
    monkeypatch.setattr(cli, 'amain', failed)
    assert cli.main(['--symbol', 'SNDK', '--hedge', 'lighter-rh', '--live', '--no-dashboard']) == 1
    assert 'runtime failure' in (tmp_path / 'engine.log').read_text()


def test_missing_live_credentials_fails_before_runtime(monkeypatch) -> None:
    monkeypatch.setattr(cli, 'load_config', lambda *args, **kwargs:
                        SimpleNamespace(creds_complete=False))
    monkeypatch.setattr(cli, 'amain', lambda *args, **kwargs: pytest.fail('must not start'))
    assert cli.main(['--symbol', 'SNDK', '--hedge', 'lighter-rh', '--live']) == 2


def test_journal_tool_acknowledgment_requires_no_pending_and_exclusive_lock(tmp_path) -> None:
    from entropy_robinhood_lighter_arbitrage.journal import OrderJournal
    from tools.journal_status import journal_status, main
    path = tmp_path / 'orders.sqlite3'
    journal = OrderJournal(str(path), 'offline-test-pair')
    journal.set_meta('halt_reason', 'test halt')
    journal.set_meta('equity_high_water', 100.)
    journal.set_meta('positions', {'entropy': 1., 'hedge': -1.})
    assert journal_status(path)['halt_reason'] == 'test halt'
    assert main([str(path), '--acknowledge-halt']) == 1  # active process owns flock
    journal.prepare_batch([{'id': 'unknown', 'qty': 1., 'limit_px': 100.,
                            'venue': 'entropy', 'is_buy': True}])
    journal.close()
    assert main([str(path), '--acknowledge-halt']) == 1
    journal = OrderJournal(str(path), 'offline-test-pair')
    journal.cancel_prepared()
    journal.close()
    assert main([str(path), '--acknowledge-halt']) == 0
    status = journal_status(path)
    assert status['halt_reason'] == '' and status['equity_high_water'] is None
    assert status['positions'] == {'entropy': 1., 'hedge': -1.}
    assert status['order_states'] == {'terminal': 1}


def test_sigterm_requests_graceful_engine_stop_without_cancelling_run(monkeypatch) -> None:
    import asyncio
    import signal

    from entropy_robinhood_lighter_arbitrage import engine

    async def scenario():
        callbacks = {}
        removed = []
        started = asyncio.Event()
        class FakeEngine:
            def __init__(self, *args, **kwargs):
                self.stop = asyncio.Event()
                self.finished = False
            def request_stop(self):
                self.stop.set()
            async def run(self):
                started.set()
                await self.stop.wait()
                self.finished = True
        monkeypatch.setattr(engine, 'Engine', FakeEngine)
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, 'add_signal_handler', lambda sig, cb: callbacks.update({sig: cb}))
        monkeypatch.setattr(loop, 'remove_signal_handler', lambda sig: removed.append(sig))
        task = asyncio.create_task(cli.amain(SimpleNamespace(), False, False, False, None, 'en'))
        await asyncio.wait_for(started.wait(), .2)
        bound_engine = callbacks[signal.SIGTERM].__self__
        callbacks[signal.SIGTERM]()
        await asyncio.wait_for(task, .2)
        assert bound_engine.finished
        assert set(removed) == {signal.SIGINT, signal.SIGTERM}
    asyncio.run(scenario())
