"""Recorded-data admission checks; no network, credentials or simulated fills."""
from __future__ import annotations

import asyncio
import csv
import warnings

import pytest

from entropy_robinhood_lighter_arbitrage.book import OrderBook
from entropy_robinhood_lighter_arbitrage.recorder import CSV_HEADER, MinuteRecorder
from tools.analyze import load_rows


def write_csv(path, *, symbol="SNDK", pair="sndk-io-rh", ts=1788012420, legacy=False):
    row = dict.fromkeys(CSV_HEADER, "0")
    row.update(schema_version="2", bar_id=f"{pair}-{ts}", pair_id=pair, symbol=symbol,
               entropy_dex="io", hedge_venue="lighter-rh", minute_ts=str(ts), samples="60",
               premium_close_bps="1", premium_mean_bps="1",
               sell_edge_max_bps="2", buy_edge_max_bps="-3")
    if legacy:
        for field in ("schema_version", "bar_id", "pair_id", "symbol", "entropy_dex", "hedge_venue"):
            row.pop(field)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def test_daily_glob_reads_single_pair_in_time_order(tmp_path):
    write_csv(tmp_path / "minutes-2.csv", ts=1788012480)
    write_csv(tmp_path / "minutes-1.csv", ts=1788012420)
    rows = load_rows(str(tmp_path / "minutes-*.csv"), 0, 10)
    assert [r["ts"] for r in rows] == [1788012420, 1788012480]
    assert rows[0]["pair_id"] == "sndk-io-rh"


def test_glob_rejects_mixed_market_identity(tmp_path):
    write_csv(tmp_path / "minutes-1.csv")
    write_csv(tmp_path / "minutes-2.csv", symbol="AAPL", pair="aapl-io-rh")
    with pytest.raises(ValueError, match="multiple market"):
        load_rows(str(tmp_path / "minutes-*.csv"), 0, 10)


def test_pair_selector_uses_explicit_single_market(tmp_path):
    write_csv(tmp_path / "minutes-1.csv")
    write_csv(tmp_path / "minutes-2.csv", symbol="AAPL", pair="aapl-io-rh")
    rows = load_rows(str(tmp_path / "minutes-*.csv"), 0, 10, pair_id="aapl-io-rh")
    assert len(rows) == 1 and rows[0]["symbol"] == "AAPL"


def test_legacy_requires_explicit_admission_and_warns(tmp_path):
    path = tmp_path / "old.csv"
    write_csv(path, legacy=True)
    with pytest.raises(ValueError, match="legacy"):
        load_rows(str(path), 0, 10)
    with pytest.warns(UserWarning, match="cannot verify"):
        rows = load_rows(str(path), 0, 10, allow_legacy=True)
    assert len(rows) == 1 and rows[0]["pair_id"] == "legacy-unverified"


def test_legacy_cannot_mix_with_identified_data(tmp_path):
    write_csv(tmp_path / "old.csv", legacy=True)
    write_csv(tmp_path / "new.csv")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pytest.raises(ValueError, match="legacy.*identified"):
            load_rows(str(tmp_path / "*.csv"), 0, 10, allow_legacy=True)


def test_invalid_numeric_data_rejected_instead_of_biasing_statistics(tmp_path):
    path = tmp_path / "bad.csv"
    write_csv(path)
    path.write_text(path.read_text().replace(",1,1,", ",nan,1,"))
    with pytest.raises(ValueError, match="finite"):
        load_rows(str(path), 0, 10)


def test_overlapping_sessions_exclude_entire_minute_with_explicit_warning(tmp_path):
    write_csv(tmp_path / "minutes-1.csv")
    write_csv(tmp_path / "minutes-2.csv")
    path = tmp_path / "minutes-2.csv"
    path.write_text(path.read_text().replace("sndk-io-rh-1788012420", "new-session"))
    with pytest.warns(UserWarning, match="excluded 1 overlapping minute"):
        rows = load_rows(str(tmp_path / "minutes-*.csv"), 0, 10)
    assert rows == []


def test_same_bar_id_with_changed_payload_is_rejected(tmp_path):
    write_csv(tmp_path / "minutes-1.csv")
    write_csv(tmp_path / "minutes-2.csv")
    path = tmp_path / "minutes-2.csv"
    path.write_text(path.read_text().replace(",60", ",59"))
    with pytest.raises(ValueError, match="conflicting duplicate bar"):
        load_rows(str(tmp_path / "minutes-*.csv"), 0, 10)


def test_clean_recorder_restart_same_minute_does_not_poison_history(tmp_path, monkeypatch):
    path = tmp_path / "minutes.csv"
    clock = [1788012430.0]
    monkeypatch.setattr("entropy_robinhood_lighter_arbitrage.recorder.time.time", lambda: clock[0])
    identity = {"pair_id": "sndk-io-rh", "symbol": "SNDK", "entropy_dex": "io",
                "hedge_venue": "lighter-rh"}

    async def record_once():
        entropy, hedge = OrderBook(), OrderBook()
        for book, bid, ask in ((entropy, 100, 100.1), (hedge, 99.9, 100)):
            book.apply_hl([[{"px": bid, "sz": 1}], [{"px": ask, "sz": 1}]])
        recorder = MinuteRecorder(str(path), entropy, hedge, 10, **identity, rotate_daily=False)
        stop = asyncio.Event()
        task = asyncio.create_task(recorder.run(stop))
        asyncio.get_running_loop().call_later(0.003, stop.set)
        await task
        assert recorder.rows_written == 1

    asyncio.run(record_once())
    clock[0] += 10
    asyncio.run(record_once())  # clean stop/restart within the same minute
    clock[0] += 60
    asyncio.run(record_once())  # a complete independent later minute remains usable
    with path.open() as fh:
        assert len(list(csv.DictReader(fh))) == 3
    with pytest.warns(UserWarning, match="excluded 1 overlapping minute"):
        rows = load_rows(str(path), 0, 1)
    assert len(rows) == 1 and rows[0]["ts"] == 1788012480
