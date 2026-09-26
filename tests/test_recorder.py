"""Minute recorder (entropy_robinhood_lighter_arbitrage.recorder): sampling + CSV contract for
tools/analyze.py."""
from __future__ import annotations

import asyncio
import csv
import os
from unittest.mock import patch

import pytest

from entropy_robinhood_lighter_arbitrage.book import OrderBook
from entropy_robinhood_lighter_arbitrage.recorder import (
    MinuteRecorder as _MinuteRecorder,
)

IDENTITY = {"pair_id": "sndk-io-lighter-rh", "symbol": "SNDK", "entropy_dex": "io",
            "hedge_venue": "lighter-rh"}
SAMPLE = {"prem": 1.0, "sell_edge": 2.0, "buy_edge": -3.0,
          "eb": 100.0, "ea": 100.1, "hb": 99.9, "ha": 100.0}


def MinuteRecorder(*args, **kwargs):
    return _MinuteRecorder(*args, **IDENTITY, rotate_daily=False, **kwargs)


def book(bids, asks) -> OrderBook:
    b = OrderBook()
    b.apply_hl([[{"px": px, "sz": sz} for px, sz in bids],
                [{"px": px, "sz": sz} for px, sz in asks]])
    return b


def test_csv_header_matches_analyze_keys(tmp_path) -> None:
    rec = MinuteRecorder(str(tmp_path / "minutes.csv"), book([], []), book([], []),
                         10.0)
    rec._minute_start = 1788012420.0
    rec._samples = [{"prem": 1.0, "sell_edge": 2.0, "buy_edge": -3.0,
                     "eb": 100.0, "ea": 100.1, "hb": 99.9, "ha": 100.0}]
    rec._flush()
    with open(tmp_path / "minutes.csv", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    r = rows[0]
    # every key tools/analyze.py reads must exist
    for k in ("minute_ts", "premium_close_bps", "premium_mean_bps",
              "sell_edge_max_bps", "buy_edge_max_bps", "samples"):
        assert k in r, f"missing analyze key {k}"
    assert r["minute_ts"] == "1788012420"
    assert r["samples"] == "1"
    assert float(r["premium_close_bps"]) == 1.0
    assert float(r["sell_edge_max_bps"]) == 2.0
    assert r["schema_version"] == "2"
    assert r["symbol"] == "SNDK" and r["hedge_venue"] == "lighter-rh"


def test_flush_empty_no_row(tmp_path) -> None:
    rec = MinuteRecorder(str(tmp_path / "minutes.csv"), book([], []), book([], []),
                         10.0)
    rec._minute_start = 1788012420.0
    rec._samples = []
    rec._flush()
    assert not os.path.exists(tmp_path / "minutes.csv") or \
        os.path.getsize(tmp_path / "minutes.csv") == 0


def test_sample_requires_both_fresh() -> None:
    eb, hb = book([(100.0, 5)], [(100.1, 5)]), book([(99.9, 5)], [(100.0, 5)])
    rec = MinuteRecorder("x.csv", eb, hb, 10.0)
    s = rec._sample()
    assert s is not None
    assert abs(s["prem"] - (100.05 / 99.95 - 1) * 1e4) < 0.01
    # one side stale -> no sample
    rec.entropy_book.alive_ts = 0.0
    assert rec._sample() is None


def test_minute_rollover_flushes(tmp_path) -> None:
    rec = MinuteRecorder(str(tmp_path / "m.csv"), book([], []), book([], []),
                         10.0)
    rec._minute_start = 1788012420.0  # 14:07:00
    rec._samples = [{"prem": 1.0, "sell_edge": 1.0, "buy_edge": -1.0,
                     "eb": 1.0, "ea": 1.0, "hb": 1.0, "ha": 1.0}]
    # simulate the run() loop boundary check
    import time
    now = time.time()
    mstart = int(now // 60) * 60
    assert rec._minute_start != mstart  # different minute -> flush would fire
    rec._flush()
    assert rec.rows_written == 1


def test_cancellation_flushes_partial_minute(tmp_path):
    async def run():
        rec = MinuteRecorder(str(tmp_path / "partial.csv"), book([], []), book([], []), 10)
        rec._sample = lambda: SAMPLE.copy()
        stop = asyncio.Event()
        task = asyncio.create_task(rec.run(stop))
        asyncio.get_running_loop().call_later(0.01, stop.set)
        await stop.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return rec
    rec = asyncio.run(run())
    assert rec.rows_written == 1
    with open(tmp_path / "partial.csv") as fh:
        assert len(list(csv.DictReader(fh))) == 1


def test_write_failure_retains_rows_and_retries(tmp_path):
    rec = MinuteRecorder(str(tmp_path / "retry.csv"), book([], []), book([], []), 10)
    rec._minute_start = 1788012420.0
    rec._samples = [SAMPLE.copy()]
    with patch.object(rec, "_write", side_effect=OSError("synthetic disk outage")):
        rec._flush()
    assert rec.pending_rows == 1 and rec.rows_written == 0
    rec._flush()
    assert rec.pending_rows == 0 and rec.rows_written == 1


def test_fsync_failure_retry_does_not_duplicate_bar(tmp_path):
    path = tmp_path / "uncertain.csv"
    rec = MinuteRecorder(str(path), book([], []), book([], []), 10)
    rec._minute_start = 1788012420.0
    rec._samples = [SAMPLE.copy()]
    with patch("entropy_robinhood_lighter_arbitrage.recorder.os.fsync",
               side_effect=OSError("synthetic fsync error")):
        rec._flush()
    assert rec.pending_rows == 1
    rec._flush()
    assert rec.rows_written == 1 and rec.pending_rows == 0
    with path.open() as fh:
        assert len(list(csv.DictReader(fh))) == 1


def test_backlog_is_bounded_without_discarding_recorded_data(tmp_path):
    rec = MinuteRecorder(str(tmp_path / "retry.csv"), book([], []), book([], []), 10,
                         max_pending_rows=1)
    with patch.object(rec, "_write", side_effect=OSError("synthetic disk outage")):
        rec._minute_start = 1788012420.0
        rec._samples = [SAMPLE.copy()]
        rec._flush()
        rec._minute_start += 60
        rec._samples = [SAMPLE.copy()]
        assert rec._flush() is False
        assert rec.pending_rows == 1 and len(rec._samples) == 1
    rec._flush()
    with open(tmp_path / "retry.csv") as fh:
        assert len(list(csv.DictReader(fh))) == 2


def test_recorder_rejects_existing_other_pair(tmp_path):
    path = tmp_path / "mixed.csv"
    rec = MinuteRecorder(str(path), book([], []), book([], []), 10)
    rec._minute_start = 1788012420.0
    rec._samples = [SAMPLE.copy()]
    rec._flush()
    with pytest.raises(ValueError, match="identity"):
        _MinuteRecorder(str(path), book([], []), book([], []), 10,
                        **(IDENTITY | {"symbol": "AAPL"}), rotate_daily=False)


def test_daily_rotation_uses_bar_date(tmp_path):
    rec = _MinuteRecorder(str(tmp_path / "minutes.csv"), book([], []), book([], []), 10,
                          **IDENTITY)
    for ts in (1788047940, 1788048000):
        rec._minute_start = ts
        rec._samples = [SAMPLE.copy()]
        rec._flush()
    files = sorted(tmp_path.glob("minutes-*.csv"))
    assert len(files) == 2
    for path in files:
        with path.open() as fh:
            assert len(list(csv.DictReader(fh))) == 1
