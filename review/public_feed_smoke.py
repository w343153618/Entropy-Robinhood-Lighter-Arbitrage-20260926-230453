"""Bounded public-data lifecycle smoke. No credentials, signers, or order sends."""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from entropy_robinhood_lighter_arbitrage.config import load_config
from entropy_robinhood_lighter_arbitrage.engine import Engine


async def smoke(args):
    cfg = load_config(args.config, '/nonexistent/credentials-never-read',
                      symbol=args.symbol, hedge_venue=args.hedge, read_credentials=False)
    out = Path(tempfile.mkdtemp(prefix='entropy-public-smoke-'))
    cfg.recorder_csv = str(out / 'minutes.csv')
    cfg.health_file = str(out / 'health.json')
    cfg.status_interval_sec = 15
    assert not cfg.creds_complete
    engine = Engine(cfg, record_only=True)
    started = time.monotonic()
    task = asyncio.create_task(engine.run())
    observations = []
    try:
        while time.monotonic() - started < args.seconds:
            if task.done():
                await task
                break
            observations.append({
                'elapsed': round(time.monotonic() - started, 3),
                'books': {k: {'ready': v.book.ready, 'fresh': v.book.is_fresh(cfg.staleness_sec),
                               'bid': v.book.best_bid(), 'ask': v.book.best_ask()}
                          for k, v in engine.venues.items()}})
            await asyncio.sleep(2)
    finally:
        engine.request_stop()
        await asyncio.wait_for(task, timeout=10)
    rows = []
    for path in out.glob('minutes-*.csv'):
        with path.open() as fh:
            rows.extend(csv.DictReader(fh))
    result = {'mode': 'public-record-only', 'pair_id': cfg.pair_id,
              'duration_sec': round(time.monotonic() - started, 2),
              'both_fresh_samples': sum(len(x['books']) == 2 and all(
                  b['fresh'] for b in x['books'].values()) for x in observations),
              'samples': len(observations), 'minute_rows': len(rows),
              'journal_opened': engine.journal is not None,
              'trades': engine.trades, 'hedges': engine.hedges,
              'all_background_tasks_done': all(t.done() for t in engine._tasks),
              'output_directory': str(out), 'observations': observations}
    Path(args.output).write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'observations'}))
    return 0 if result['both_fresh_samples'] and rows else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config.example.yaml')
    parser.add_argument('--symbol', default='SNDK')
    parser.add_argument('--hedge', default='lighter-rh')
    parser.add_argument('--seconds', type=float, default=75)
    parser.add_argument('--output', default='review/public-feed-smoke.json')
    args = parser.parse_args()
    if not 2 <= args.seconds <= 300:
        parser.error('--seconds must be between 2 and 300')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    raise SystemExit(asyncio.run(smoke(args)))
