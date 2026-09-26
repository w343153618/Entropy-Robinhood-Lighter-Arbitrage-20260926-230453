#!/usr/bin/env python3
"""Inspect the journal read-only; explicit halt acknowledgment requires a stopped bot."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def journal_status(path: str | Path) -> dict:
    path = Path(path).resolve()
    if not path.is_file():
        raise ValueError('journal not found; refusing to create a replacement')
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as db:
        metadata = {key: json.loads(value) for key, value in
                    db.execute('SELECT key,value FROM metadata')}
        states = dict(db.execute('SELECT state,count(*) FROM orders GROUP BY state'))
    return {'path': str(path), 'identity': metadata.get('identity'),
            'halt_reason': metadata.get('halt_reason', ''),
            'equity_high_water': metadata.get('equity_high_water'),
            'positions': metadata.get('positions'), 'order_states': states,
            'pending_orders': sum(count for state, count in states.items() if state != 'terminal')}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path', help='existing runtime.state_db; never delete it to resume')
    parser.add_argument('--acknowledge-halt', action='store_true',
                        help='after manually verifying both venue accounts, clear halt and reset equity peak')
    args = parser.parse_args(argv)
    try:
        status = journal_status(args.path)
        if args.acknowledge_halt:
            print('Operator acknowledgment: both venue accounts and collateral must already '
                  'be reconciled; this command does not verify them or flatten positions.', file=sys.stderr)
            # Import after the read-only existence check; direct script use from checkout.
            sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from entropy_robinhood_lighter_arbitrage.journal import OrderJournal
            if not isinstance(status['identity'], str):
                raise ValueError('journal identity missing or invalid')
            journal = OrderJournal(args.path, status['identity'])
            try:
                journal.acknowledge_halt()
            finally:
                journal.close()
            status = journal_status(args.path)
            status['halt_acknowledged'] = True
        print(json.dumps(status, indent=2, ensure_ascii=False, allow_nan=False))
        return 0
    except (OSError, ValueError, sqlite3.Error, RuntimeError) as exc:
        print(f'journal error: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
