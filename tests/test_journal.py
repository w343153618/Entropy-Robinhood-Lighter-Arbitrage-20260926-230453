import signal
import subprocess
import sys
from pathlib import Path

import pytest

from entropy_robinhood_lighter_arbitrage.journal import JournalError, OrderJournal


def intent(oid="one"):
    return {"id": oid, "venue": "entropy", "is_buy": True, "qty": 1.0,
            "limit_px": 100.0, "reduce_only": False, "batch_id": "pair-1"}


def test_crash_recovery_preserves_submitted_and_cancels_only_unsent(tmp_path):
    path = tmp_path / "orders.sqlite3"
    j = OrderJournal(str(path), "account+pair")
    j.prepare_batch([intent("one"), intent("two")])
    j.mark_submitted("one")
    j.close()
    recovered = OrderJournal(str(path), "account+pair")
    recovered.cancel_prepared()
    assert [o["id"] for o in recovered.pending()] == ["one"]
    recovered.close()


def test_terminal_result_idempotent_and_cannot_be_overwritten(tmp_path):
    j = OrderJournal(str(tmp_path / "orders.sqlite3"), "pair")
    j.prepare_batch([intent()])
    j.mark_submitted("one")
    result = {"status": "filled", "filled_base": 1.0, "avg_px": 100.0,
              "err": None, "unresolved": False}
    assert j.record_result("one", result)
    assert not j.record_result("one", result)
    with pytest.raises(JournalError):
        j.record_result("one", {**result, "filled_base": 0.0})
    assert j.pending() == []
    j.close()


def test_identity_and_exclusive_writer_guard(tmp_path):
    path = str(tmp_path / "orders.sqlite3")
    j = OrderJournal(path, "one")
    with pytest.raises(JournalError):
        OrderJournal(path, "one")
    j.set_meta("halt_reason", "loss limit")
    j.close()
    with pytest.raises(JournalError):
        OrderJournal(path, "another-account")
    reopened = OrderJournal(path, "one")
    assert reopened.get_meta("halt_reason") == "loss limit"
    reopened.close()


def test_prepare_batch_is_atomic_on_duplicate_id(tmp_path):
    j = OrderJournal(str(tmp_path / "orders.sqlite3"), "pair")
    with pytest.raises(JournalError):
        j.prepare_batch([intent(), intent()])
    assert j.pending() == []
    j.close()


@pytest.mark.parametrize("submitted", [False, True])
def test_sigkill_process_preserves_committed_intents_and_positions(tmp_path, submitted):
    """A real abrupt child-process death; no clean close/checkpoint is possible."""
    path = str(tmp_path / "orders.sqlite3")
    code = '''
import os, signal, sys
from entropy_robinhood_lighter_arbitrage.journal import OrderJournal
j = OrderJournal(sys.argv[1], "crash-pair")
j.set_meta("positions", {"entropy": 0.0, "hedge": 0.0})
orders = [{"id": key, "venue": key, "qty": 1, "limit_px": 100,
           "is_buy": key == "entropy", "reduce_only": False, "batch_id": "pair"}
          for key in ("entropy", "hedge")]
j.prepare_batch(orders)
if sys.argv[2] == "True":
    for o in orders:
        j.mark_submitted(o["id"])
    j.record_result("entropy", {"status": "filled", "filled_base": 1.0,
                                "avg_px": 100.0, "unresolved": False})
os.kill(os.getpid(), signal.SIGKILL)
'''
    result = subprocess.run([sys.executable, '-c', code, path, str(submitted)],
                            cwd=Path(__file__).resolve().parents[1],
                            capture_output=True, timeout=5, check=False)
    assert result.returncode == -signal.SIGKILL, result.stderr.decode()
    recovered = OrderJournal(path, 'crash-pair')
    try:
        recovered.cancel_prepared()
        assert recovered.get_meta('positions')['entropy'] == int(submitted)
        assert [o['id'] for o in recovered.pending()] == (['hedge'] if submitted else [])
    finally:
        recovered.close()
