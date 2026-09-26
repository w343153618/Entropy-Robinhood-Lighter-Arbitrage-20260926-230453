"""Pair-identified minute bars, UTC daily rotation and bounded write retries.

The CSV measures observed book opportunities; it is not a fill simulation.
Samples and queued bars survive transient write failures in memory. Collection
pauses under sustained storage failure instead of growing an unbounded buffer.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import math
import os
import time
import uuid
from collections import deque
from pathlib import Path

from .book import OrderBook

log = logging.getLogger("recorder")

SCHEMA_VERSION = "2"
IDENTITY_FIELDS = ("pair_id", "symbol", "entropy_dex", "hedge_venue")
CSV_HEADER = [
    "schema_version", "bar_id", *IDENTITY_FIELDS,
    "minute_ts", "time_utc",
    "entropy_bid", "entropy_ask", "hedge_bid", "hedge_ask",
    "premium_open_bps", "premium_high_bps", "premium_low_bps",
    "premium_close_bps", "premium_mean_bps", "premium_std_bps",
    "sell_edge_mean_bps", "sell_edge_max_bps",
    "buy_edge_mean_bps", "buy_edge_max_bps", "samples",
]
SAMPLE_INTERVAL = 1.0
MINUTE = 60.0
MAX_MINUTE_SAMPLES = 120


class MinuteRecorder:
    def __init__(self, csv_path: str, entropy_book: OrderBook,
                 hedge_book: OrderBook, staleness_sec: float, *,
                 pair_id: str, symbol: str, entropy_dex: str, hedge_venue: str,
                 rotate_daily: bool = True, max_pending_rows: int = 1440) -> None:
        if not isinstance(max_pending_rows, int) or isinstance(max_pending_rows, bool) \
                or max_pending_rows < 1:
            raise ValueError("recorder max_pending_rows must be a positive integer")
        self.identity = {"pair_id": pair_id, "symbol": symbol,
                         "entropy_dex": entropy_dex, "hedge_venue": hedge_venue}
        if any(not isinstance(value, str) or not value for value in self.identity.values()):
            raise ValueError("recorder market identity fields must be nonempty strings")
        self.csv_path = csv_path
        self.rotate_daily = rotate_daily
        self.max_pending_rows = max_pending_rows
        self.entropy_book = entropy_book
        self.hedge_book = hedge_book
        self.staleness_sec = staleness_sec
        self.rows_written = 0
        self.write_errors = 0
        self.last_write_error: str | None = None
        self.paused_samples = 0
        self._minute_start: float | None = None
        self._samples: list[dict] = []
        self._pending: deque[dict] = deque()
        self._validate_existing(Path(csv_path))
        if rotate_daily:
            self._validate_existing(self._path_for(time.time()))

    @property
    def pending_rows(self) -> int:
        return len(self._pending)

    def _path_for(self, ts: float) -> Path:
        path = Path(self.csv_path)
        if not self.rotate_daily:
            return path
        date = time.strftime("%Y-%m-%d", time.gmtime(ts))
        return path.with_name(f"{path.stem}-{date}{path.suffix or '.csv'}")

    def _check_reader(self, reader: csv.DictReader) -> set[str]:
        if reader.fieldnames != CSV_HEADER:
            raise ValueError("recorder CSV schema mismatch; use a new pair-specific file")
        bar_ids = set()
        for row in reader:
            if row.get("schema_version") != SCHEMA_VERSION or \
                    any(row.get(key) != value for key, value in self.identity.items()):
                raise ValueError("recorder CSV market identity mismatch; refusing mixed data")
            if not row.get("bar_id") or row.get("samples") is None or None in row:
                raise ValueError("recorder CSV contains an incomplete row")
            bar_ids.add(row["bar_id"])
        return bar_ids

    def _validate_existing(self, path: Path) -> None:
        if path.exists() and path.stat().st_size:
            with path.open(newline="") as fh:
                self._check_reader(csv.DictReader(fh))

    def _sample(self) -> dict | None:
        if not (self.entropy_book.is_fresh(self.staleness_sec)
                and self.hedge_book.is_fresh(self.staleness_sec)):
            return None
        eb, ea = self.entropy_book.best_bid(), self.entropy_book.best_ask()
        hb, ha = self.hedge_book.best_bid(), self.hedge_book.best_ask()
        prices = (eb, ea, hb, ha)
        if any(px is None or not math.isfinite(px) or px <= 0 for px in prices):
            return None
        if eb > ea or hb > ha:
            return None
        mid_e, mid_h = (eb + ea) / 2.0, (hb + ha) / 2.0
        return {"prem": (mid_e / mid_h - 1.0) * 1e4,
                "sell_edge": (eb / ha - 1.0) * 1e4,
                "buy_edge": (hb / ea - 1.0) * 1e4,
                "eb": eb, "ea": ea, "hb": hb, "ha": ha}

    def _bar(self) -> dict:
        rows = self._samples
        ts = self._minute_start
        prem = [r["prem"] for r in rows]
        mean = sum(prem) / len(prem)
        var = sum((x - mean) ** 2 for x in prem) / len(prem)
        sell = [r["sell_edge"] for r in rows]
        buy = [r["buy_edge"] for r in rows]
        return {
            "schema_version": SCHEMA_VERSION, "bar_id": uuid.uuid4().hex,
            **self.identity,
            "minute_ts": int(ts),
            "time_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)),
            "entropy_bid": rows[-1]["eb"], "entropy_ask": rows[-1]["ea"],
            "hedge_bid": rows[-1]["hb"], "hedge_ask": rows[-1]["ha"],
            "premium_open_bps": round(prem[0], 4),
            "premium_high_bps": round(max(prem), 4),
            "premium_low_bps": round(min(prem), 4),
            "premium_close_bps": round(prem[-1], 4),
            "premium_mean_bps": round(mean, 4),
            "premium_std_bps": round(math.sqrt(var), 4),
            "sell_edge_mean_bps": round(sum(sell) / len(sell), 4),
            "sell_edge_max_bps": round(max(sell), 4),
            "buy_edge_mean_bps": round(sum(buy) / len(buy), 4),
            "buy_edge_max_bps": round(max(buy), 4), "samples": len(rows),
        }

    def _drain_pending(self) -> None:
        while self._pending:
            try:
                self._write([self._pending[0]])
            except OSError as exc:
                self.write_errors += 1
                # Retain data and report once per failure state, not every second.
                message = f"{type(exc).__name__}: {exc}"
                if message != self.last_write_error:
                    log.error("recorder write failed; retaining %d queued bar(s): %s",
                              len(self._pending), message)
                self.last_write_error = message
                return
            self._pending.popleft()
            self.rows_written += 1
        if self.last_write_error:
            log.info("recorder storage recovered; buffered bars flushed")
        self.last_write_error = None

    def _flush(self) -> bool:
        """Returns False under backpressure; never clears unqueued samples."""
        self._drain_pending()
        if self._samples:
            if len(self._pending) >= self.max_pending_rows:
                return False
            self._pending.append(self._bar())
            self._samples = []
        self._drain_pending()
        return True

    def _write(self, rows: list[dict]) -> None:
        for row in rows:
            path = self._path_for(row["minute_ts"])
            path.parent.mkdir(parents=True, exist_ok=True)
            # Existing rows are checked before appending. Stable bar_id makes a
            # retry after flush/fsync ambiguity idempotent within the CSV file.
            with path.open("a+", newline="") as fh:
                fh.seek(0)
                first = fh.read(1)
                fh.seek(0)
                existing = self._check_reader(csv.DictReader(fh)) if first else set()
                if row["bar_id"] in existing:
                    # A preceding fsync may have failed after the line reached
                    # the OS cache. Confirm durability before dropping the retry.
                    fh.flush()
                    os.fsync(fh.fileno())
                    continue
                fh.seek(0, os.SEEK_END)
                writer = csv.DictWriter(fh, fieldnames=CSV_HEADER)
                if not first:
                    writer.writeheader()
                writer.writerow(row)
                fh.flush()
                os.fsync(fh.fileno())

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                self._drain_pending()
                now = time.time()
                mstart = int(now // MINUTE) * MINUTE
                blocked = False
                if self._minute_start is None:
                    self._minute_start = mstart
                elif mstart != self._minute_start:
                    blocked = not self._flush()
                    if not blocked:
                        self._minute_start = mstart
                if blocked or len(self._samples) >= MAX_MINUTE_SAMPLES:
                    self.paused_samples += 1
                else:
                    sample = self._sample()
                    if sample is not None:
                        self._samples.append(sample)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=SAMPLE_INTERVAL)
                except TimeoutError:
                    pass
        finally:
            self._flush()
            if self._pending or self._samples:
                log.error("recorder stopped with %d unwritten bars and %d samples; "
                          "storage remains unavailable", len(self._pending), len(self._samples))
