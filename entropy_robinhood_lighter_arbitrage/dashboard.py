"""Rich terminal dashboard.

Shows both books with age/spread, positions and caps, equity and session PnL,
the executable premium of each direction against its full hurdle (fees and
inventory surcharge included, ● = armed), recorder progress, the last
executions, and a tail of the log (the full log goes to logging.file).
--cn displays it in Chinese. Off-terminal runs fall back to plain console
logs automatically (main.py handles that); --no-dashboard forces it.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .engine import Engine

LANGS = {
    "en": {
        "books": "BOOKS", "premium": "PREMIUM (executable, net of fees+inventory)",
        "positions": "POSITIONS / CAPS", "equity": "EQUITY", "pnl": "SESSION PnL",
        "recorder": "RECORDER", "last_executions": "LAST EXECUTIONS",
        "log": "LOG", "armed": "● armed", "stale": "STALE", "down": "DOWN",
        "rate_limited": "RATE-LTD", "sell": "SELL entropy", "buy": "BUY entropy",
        "min": "min", "age": "age", "spread": "spr", "rows": "rows",
        "bids": "bids", "asks": "asks", "status": "status", "exp": "exp",
        "fill": "fill", "prem": "prem", "none": "—",
    },
    "zh": {
        "books": "盘口", "premium": "可执行溢价（已扣费用+库存附加）",
        "positions": "持仓 / 上限", "equity": "账户权益", "pnl": "会话盈亏",
        "recorder": "数据采集", "last_executions": "最近成交",
        "log": "日志", "armed": "● 已就绪", "stale": "过期", "down": "宕机",
        "rate_limited": "限频", "sell": "卖 Entropy", "buy": "买 Entropy",
        "min": "分钟", "age": "时效", "spread": "价差", "rows": "行",
        "bids": "买", "asks": "卖", "status": "状态", "exp": "预期",
        "fill": "成交", "prem": "溢价", "none": "—",
    },
}


class BufferLogHandler(logging.Handler):
    """Log handler that keeps the last N formatted lines for the dashboard."""

    def __init__(self, capacity: int = 100) -> None:
        super().__init__()
        self.lines: deque[str] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.lines.append(self.format(record))
        except Exception:
            pass


class Dashboard:
    def __init__(self, engine: Engine, log_buffer: BufferLogHandler | None,
                 log_file: str | None, force_terminal: bool = False,
                 lang: str = "en") -> None:
        self.engine = engine
        self.log_buffer = log_buffer
        self.lang = lang
        self.txt = LANGS.get(lang, LANGS["en"])
        self.console = Console(force_terminal=force_terminal)

    # -------------------------------------------------------------- helpers

    def _fmt(self, x, nd: int = 4) -> str:
        if x is None:
            return self.txt["none"]
        return f"{x:.{nd}f}"

    def _age(self, ts: float) -> str:
        if ts <= 0:
            return self.txt["none"]
        return f"{time.time() - ts:.0f}s"

    def _status_panel(self) -> Panel:
        eng = self.engine
        if eng.stop.is_set():
            state = "STOPPED" if getattr(eng, "_shutdown_done", False) else "STOPPING"
            reason = eng.halt_reason or eng.recovery_reason
        elif eng.record_only:
            state, reason = "RECORD_ONLY", ""
        elif eng.halted:
            state, reason = "HALTED", eng.halt_reason
        elif eng.recovering:
            state, reason = "RECOVERING", eng.recovery_reason
        elif getattr(eng, "_blocked_reason", ""):
            state, reason = "PAUSED", eng._blocked_reason
        else:
            state, reason = "READY", ""
        pending = len(eng.journal.pending()) if eng.journal is not None else 0
        return Panel(Text(f"{state} | pending={pending} | {reason}"),
                     title=self.txt["status"])

    def _books_panel(self) -> Panel:
        t = Table(show_header=True, header_style="bold", expand=True)
        t.add_column(self.txt["books"], style="bold")
        t.add_column("bid")
        t.add_column("ask")
        t.add_column(f"{self.txt['spread']} bps")
        t.add_column(f"{self.txt['age']}")
        for v in self.engine.venues.values():
            b, a = v.book.best_bid(), v.book.best_ask()
            spread = (a / b - 1.0) * 1e4 if b and a else None
            age = "" if v.book.is_fresh(self.engine.cfg.staleness_sec) \
                else f" {self.txt['stale']}"
            t.add_row(v.name, self._fmt(b), self._fmt(a),
                      self._fmt(spread, 2), self._age(v.book.last_update_ts) + age)
        return Panel(t, title=self.txt["books"])

    def _premium_panel(self) -> Panel:
        eng = self.engine
        t = Table(show_header=True, header_style="bold", expand=True)
        t.add_column("")
        t.add_column("mid prem (bps)")
        t.add_column("hurdle (bps)")
        t.add_column("edge (bps)")
        t.add_column("")
        for dkey, label, buy_v, sell_v in (
                ("sell_entropy", self.txt["sell"], eng.hedge, eng.entropy),
                ("buy_entropy", self.txt["buy"], eng.entropy, eng.hedge)):
            edge = self._exec_premium(buy_v, sell_v)
            hurdle = eng._eff_threshold(buy_v, sell_v)
            # fees are inside the plan, not the threshold; show threshold + fees
            hurdle += (eng.entropy.fee_bps + eng.hedge.fee_bps)
            armed = eng._armed.get(dkey) is not None
            mark = self.txt["armed"] if armed else ""
            t.add_row(label, self._fmt(eng.premium_bps(), 2),
                      self._fmt(hurdle, 2), self._fmt(edge, 2), mark)
        return Panel(t, title=self.txt["premium"])

    def _exec_premium(self, buy, sell) -> float | None:
        """Executable premium for 'sell on sell venue / buy on buy venue'."""
        b, a = sell.book.best_bid(), buy.book.best_ask()
        if not (b and a):
            return None
        return (b / a - 1.0) * 1e4

    def _positions_panel(self) -> Panel:
        eng = self.engine
        t = Table(show_header=True, header_style="bold", expand=True)
        t.add_column("")
        t.add_column("pos")
        t.add_column("cap $")
        t.add_column(f"{self.txt['status']}")
        net = sum(v.position for v in eng.venues.values())
        for v in eng.venues.values():
            status = ""
            if eng._venue_limited(v):
                status = self.txt["rate_limited"]
            elif v.key in eng._venue_down:
                status = self.txt["down"]
            elif not v.book.is_fresh(eng.cfg.staleness_sec):
                status = self.txt["stale"]
            t.add_row(v.name, f"{v.position:+.6g}", f"{v.cap_usd:.0f}", status)
        t.add_row("net", f"{net:+.6g}", "", "")
        if eng.halted:
            t.add_row("HALTED", "", "", eng.halt_reason)
        elif eng.recovering:
            t.add_row("RECOVERING", "", "", eng.recovery_reason)
        else:
            t.add_row("RECORD_ONLY" if eng.record_only else "READY", "", "", "")
        if eng.journal is not None:
            t.add_row("pending", str(len(eng.journal.pending())), "", "")
        return Panel(t, title=self.txt["positions"])

    def _equity_panel(self) -> Panel:
        eng = self.engine
        t = Table(show_header=False, expand=True)
        for v in eng.venues.values():
            eq = f"${v.equity:,.2f}" if v.equity is not None else self.txt["none"]
            free = f"${v.free:,.2f}" if v.free is not None else self.txt["none"]
            t.add_row(v.name, eq, f"free {free}")
        pnl = eng.session_pnl()
        delta = eng.account_delta()
        pnl_s = f"${pnl:+.4f}" if pnl is not None else self.txt["none"]
        delta_s = f"${delta:+.2f}" if delta is not None else self.txt["none"]
        t.add_row(self.txt["pnl"], pnl_s, f"acct {delta_s}")
        return Panel(t, title=f"{self.txt['equity']} / {self.txt['pnl']}")

    def _recorder_panel(self) -> Panel:
        eng = self.engine
        if eng.recorder is None:
            return Panel("—", title=self.txt["recorder"])
        r = eng.recorder
        n = r.rows_written
        return Panel(f"{n} {self.txt['rows']} ({len(r._samples)}s "
                     f"this {self.txt['min']})", title=self.txt["recorder"])

    def _trades_panel(self) -> Panel:
        eng = self.engine
        t = Table(show_header=True, header_style="bold", expand=True)
        t.add_column("ts")
        t.add_column("dir")
        t.add_column("qty")
        t.add_column(f"{self.txt['prem']} bps")
        t.add_column(f"{self.txt['exp']} $")
        t.add_column(f"{self.txt['fill']} $")
        t.add_column(f"{self.txt['status']}")
        for tr in reversed(list(eng.recent_trades)):
            t.add_row(time.strftime("%H:%M:%S", time.localtime(tr["ts"])),
                      tr["direction"], f"{tr['qty']:.4g}",
                      f"{tr['prem_bps']:.2f}",
                      self._fmt(tr["exp"], 4), self._fmt(tr["fill"], 4),
                      tr["status"])
        return Panel(t, title=self.txt["last_executions"])

    def _log_panel(self) -> Panel:
        if self.log_buffer is None:
            return Panel("", title=self.txt["log"])
        lines = "\n".join(list(self.log_buffer.lines)[-25:])
        return Panel(lines, title=self.txt["log"])

    # ---------------------------------------------------------------- runner

    async def run(self) -> None:
        refresh = 1.0
        layout = Layout()
        layout.split_column(
            Layout(name="status", size=3),
            Layout(name="top", size=8),
            Layout(name="middle"),
            Layout(name="log", size=8),
        )
        layout["top"].split_row(
            Layout(name="books"), Layout(name="premium"), Layout(name="positions"),
            Layout(name="equity"),
        )
        layout["middle"].split_row(
            Layout(name="recorder"), Layout(name="trades"),
        )
        try:
            with Live(layout, console=self.console, refresh_per_second=2,
                      screen=True) as live:
                while not self.engine.stop.is_set():
                    layout["status"].update(self._status_panel())
                    layout["books"].update(self._books_panel())
                    layout["premium"].update(self._premium_panel())
                    layout["positions"].update(self._positions_panel())
                    layout["equity"].update(self._equity_panel())
                    layout["recorder"].update(self._recorder_panel())
                    layout["trades"].update(self._trades_panel())
                    layout["log"].update(self._log_panel())
                    live.update(layout)
                    try:
                        await asyncio.wait_for(self.engine.stop.wait(),
                                               timeout=refresh)
                    except TimeoutError:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger("dashboard").exception("dashboard stopped after render failure")
