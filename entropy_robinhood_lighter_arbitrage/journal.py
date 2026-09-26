"""Durable order intents and terminal outcomes, written before any network send.

The database belongs to one account/pair and one process. Unknown outcomes are
never inferred from an empty position or a missing order lookup. SQLite must be
on local persistent storage; deleting this file discards recovery information.
"""
from __future__ import annotations

import fcntl
import json
import math
import os
import sqlite3
import time
from pathlib import Path


class JournalError(RuntimeError):
    pass


class OrderJournal:
    def __init__(self, path: str, identity: str) -> None:
        self.path = str(Path(path).resolve())
        self._db = None
        self._lock = None
        try:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
            self._lock = os.fdopen(fd, "a+")
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._db = sqlite3.connect(self.path, timeout=5)
            os.chmod(self.path, 0o600)
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            if self._db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise JournalError("order journal integrity check failed")
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY, created REAL NOT NULL,
                    updated REAL NOT NULL, state TEXT NOT NULL,
                    intent TEXT NOT NULL, result TEXT);
                CREATE INDEX IF NOT EXISTS orders_state ON orders(state);
            """)
            saved = self.get_meta("identity")
            if saved is not None and saved != identity:
                raise JournalError("order journal belongs to a different account/pair")
            if self.get_meta("schema_version", 1) != 1:
                raise JournalError("unsupported order journal schema; do not downgrade this database")
            self.set_meta("identity", identity)
            self.set_meta("schema_version", 1)
        except Exception as exc:
            self.close()
            if isinstance(exc, JournalError):
                raise
            raise JournalError("cannot open order journal (locked, corrupt, or unwritable)") from exc

    def get_meta(self, key: str, default=None):
        row = self._db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key: str, value) -> None:
        try:
            encoded = json.dumps(value, allow_nan=False)
            with self._db:
                self._db.execute("INSERT INTO metadata(key,value) VALUES(?,?) "
                                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                                 (key, encoded))
        except (sqlite3.Error, ValueError) as exc:
            raise JournalError("cannot persist journal metadata") from exc

    def prepare_batch(self, intents: list[dict]) -> None:
        """Both arbitrage legs are committed atomically before either is sent."""
        now = time.time()
        try:
            for order in intents:
                for field in ("qty", "limit_px"):
                    if (isinstance(order[field], bool) or not math.isfinite(order[field])
                            or order[field] <= 0):
                        raise JournalError("invalid order intent")
            with self._db:
                self._db.executemany(
                    "INSERT INTO orders VALUES(?,?,?,?,?,NULL)",
                    [(str(o["id"]), now, now, "prepared", json.dumps(o, allow_nan=False))
                     for o in intents])
        except (sqlite3.Error, ValueError, KeyError, TypeError) as exc:
            raise JournalError("cannot persist order intents; no order may be sent") from exc

    def mark_submitted(self, order_id: str) -> None:
        try:
            with self._db:
                cur = self._db.execute(
                    "UPDATE orders SET state='submitted',updated=? WHERE id=? AND state='prepared'",
                    (time.time(), str(order_id)))
                if cur.rowcount != 1:
                    raise JournalError("order intent missing or already submitted")
        except sqlite3.Error as exc:
            raise JournalError("cannot persist submission marker") from exc

    def record_result(self, order_id: str, result: dict) -> bool:
        """True only for a first terminal result. Replays cannot double-book a fill."""
        try:
            encoded = json.dumps(result, allow_nan=False, sort_keys=True)
            with self._db:
                row = self._db.execute("SELECT state,result FROM orders WHERE id=?",
                                       (str(order_id),)).fetchone()
                if row is None:
                    raise JournalError("result has no durable intent")
                if row["state"] == "terminal":
                    previous = json.loads(row["result"])
                    # Error text/latency can differ across equivalent lookups.
                    if any(previous.get(k) != result.get(k)
                           for k in ("filled_base", "avg_px", "unresolved")):
                        raise JournalError("conflicting terminal results for one order")
                    return False
                terminal = not result.get("unresolved", True)
                self._db.execute("UPDATE orders SET state=?,updated=?,result=? WHERE id=?",
                                 ("terminal" if terminal else "unknown", time.time(),
                                  encoded, str(order_id)))
                if terminal:
                    # Expected positions and the outcome commit together, so a
                    # crash between recording and updating RAM cannot lose a fill.
                    positions = self.get_meta("positions")
                    if positions is not None:
                        intent_row = self._db.execute("SELECT intent FROM orders WHERE id=?",
                                                      (str(order_id),)).fetchone()
                        intent = json.loads(intent_row[0])
                        fill = float(result["filled_base"])
                        positions[intent["venue"]] += fill if intent["is_buy"] else -fill
                        self._db.execute("UPDATE metadata SET value=? WHERE key='positions'",
                                         (json.dumps(positions, allow_nan=False),))
                return terminal
        except (sqlite3.Error, ValueError) as exc:
            raise JournalError("cannot persist order outcome") from exc

    def pending(self) -> list[dict]:
        rows = self._db.execute("SELECT * FROM orders WHERE state!='terminal' ORDER BY created,id")
        return [{**json.loads(r["intent"]), "created": r["created"],
                 "state": r["state"], "result": json.loads(r["result"]) if r["result"] else None}
                for r in rows]

    def cancel_prepared(self) -> None:
        """Only intents without a submission marker are provably never sent."""
        result = json.dumps({"status": "not-submitted", "filled_base": 0.0,
                             "avg_px": None, "err": None, "unresolved": False})
        with self._db:
            self._db.execute("UPDATE orders SET state='terminal',updated=?,result=? "
                             "WHERE state='prepared'", (time.time(), result))

    def acknowledge_halt(self) -> None:
        """Explicit operator action, available only with this exclusive lock."""
        if self.pending():
            raise JournalError("cannot acknowledge halt while order outcomes are pending")
        with self._db:
            self._db.executemany(
                "INSERT INTO metadata(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [("halt_reason", json.dumps("")), ("equity_high_water", "null")])

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._lock is not None:
            # Closing releases the flock even during interpreter shutdown.
            self._lock.close()
            self._lock = None
