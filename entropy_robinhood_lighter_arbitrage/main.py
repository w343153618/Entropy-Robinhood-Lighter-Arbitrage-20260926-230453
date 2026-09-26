"""Installed CLI: explicitly select live trading, recording, or offline validation."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from collections.abc import Sequence
from logging.handlers import RotatingFileHandler

from .config import HEDGE_VENUES, ConfigError, load_config


def close_logging() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, '_arb_managed', False):
            root.removeHandler(handler)
            handler.close()


def setup_logging(level: str, log_file: str | None = None,
                  extra_handler: logging.Handler | None = None, *,
                  max_bytes: int = 10485760, backup_count: int = 5,
                  console: bool = True) -> None:
    close_logging()
    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03dZ %(levelname)-7s %(name)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')
    formatter.converter = time.gmtime
    handlers: list[logging.Handler] = []
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        handlers.append(RotatingFileHandler(log_file, maxBytes=max_bytes,
                                           backupCount=backup_count, encoding='utf-8'))
    if console:
        handlers.append(logging.StreamHandler())
    if extra_handler is not None:
        handlers.append(extra_handler)
    for handler in handlers:
        handler._arb_managed = True
        handler.setFormatter(formatter)
        root.addHandler(handler)
    logging.getLogger('websockets').setLevel(logging.WARNING)


async def amain(cfg, record_only: bool, use_dashboard: bool, force_tty: bool,
                log_buffer, lang: str) -> None:
    from .engine import Engine
    eng = Engine(cfg, record_only=record_only)
    loop = asyncio.get_running_loop()
    installed_signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, eng.request_stop)
            installed_signals.append(sig)
        except NotImplementedError:
            logging.getLogger('main').warning('signal handlers unavailable on this platform')
    dash_task = None
    try:
        if use_dashboard:
            from .dashboard import Dashboard
            dash = Dashboard(eng, log_buffer, cfg.log_file, force_terminal=force_tty, lang=lang)
            dash_task = asyncio.create_task(dash.run(), name='dashboard')
        await eng.run()
    finally:
        eng.request_stop()
        if dash_task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(dash_task), timeout=5)
            except Exception:
                if not dash_task.done():
                    dash_task.cancel()
            await asyncio.gather(dash_task, return_exceptions=True)
        for sig in installed_signals:
            loop.remove_signal_handler(sig)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='Two-venue arbitrage; live trading requires --live.')
    p.add_argument('--symbol', required=True, help='symbol on both venues, e.g. SNDK')
    p.add_argument('--hedge', required=True, choices=HEDGE_VENUES,
                   help='hedge deployment; Entropy is always the other venue')
    p.add_argument('--config', default='config.yaml', help='strategy YAML')
    p.add_argument('--env-file', default='.env', help='credential file, loaded only with --live')
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--live', action='store_true', help='send real orders using both venue credentials')
    mode.add_argument('--record-only', action='store_true', help='collect data; no strategy or signing')
    mode.add_argument('--check-config', action='store_true', help='validate configuration offline; no credentials or network')
    p.add_argument('--cn', action='store_true', help='Chinese dashboard')
    display = p.add_mutually_exclusive_group()
    display.add_argument('--dashboard', action='store_true', help='force Rich dashboard')
    display.add_argument('--no-dashboard', action='store_true', help='console and rotating file logs')
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        cfg = load_config(args.config, args.env_file, symbol=args.symbol,
                          hedge_venue=args.hedge, read_credentials=args.live)
    except (ConfigError, OSError) as exc:
        print(f'config error: {exc}', file=sys.stderr)
        return 2
    if args.check_config:
        print(f'config valid: pair={cfg.pair_id}\nstate={cfg.state_db}\nhealth={cfg.health_file}')
        return 0
    if args.live and not cfg.creds_complete:
        print('config error: --live requires credentials for both venues', file=sys.stderr)
        return 2
    use_dashboard = ((cfg.dashboard or args.dashboard) and not args.no_dashboard
                     and (sys.stdout.isatty() or args.dashboard))
    log_buffer = None
    if use_dashboard:
        try:
            from .dashboard import BufferLogHandler
            log_buffer = BufferLogHandler()
        except ImportError:
            print('rich unavailable; using console and file logs', file=sys.stderr)
            use_dashboard = False
    try:
        setup_logging(cfg.log_level, cfg.log_file, log_buffer,
                      max_bytes=cfg.log_max_bytes, backup_count=cfg.log_backup_count,
                      console=not use_dashboard)
        asyncio.run(amain(cfg, record_only=args.record_only, use_dashboard=use_dashboard,
                          force_tty=args.dashboard, log_buffer=log_buffer,
                          lang='zh' if args.cn else 'en'))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:
        logging.getLogger('main').exception('runtime failure; inspect state and health before restarting')
        return 1
    finally:
        close_logging()


if __name__ == '__main__':
    raise SystemExit(main())
