#!/usr/bin/env python3
"""Read-only health alarm. A degraded result must not trigger a blind restart."""
from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

STATES = {'READY', 'PAUSED', 'RECOVERING', 'HALTED', 'STOPPED', 'RECORD_ONLY'}


def _number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def check_health(path: str | Path, *, max_age: float = 15.0,
                 now: float | None = None, pair_id: str | None = None) -> tuple[int, str]:
    if not _number(max_age) or max_age <= 0:
        return 2, 'INVALID: max-age must be positive and finite'
    try:
        data = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return 2, 'UNAVAILABLE: health file missing, unreadable or incomplete'
    fields = {'schema_version', 'ts', 'pair_id', 'state', 'halt_reason', 'recovery_reason',
              'pending_orders', 'net_base', 'net_usd', 'books_fresh', 'stopped'}
    if not isinstance(data, dict) or not fields <= data.keys():
        return 2, 'INVALID: incomplete health schema'
    fresh = data['books_fresh']
    if (type(data['schema_version']) is not int or data['schema_version'] != 1
            or not _number(data['ts']) or not _number(data['net_base'])
            or (data['net_usd'] is not None and not _number(data['net_usd']))
            or not isinstance(data['pair_id'], str) or not data['pair_id']
            or not isinstance(data['state'], str) or data['state'] not in STATES
            or type(data['stopped']) is not bool
            or type(data['pending_orders']) is not int or data['pending_orders'] < 0
            or not isinstance(data['halt_reason'], str) or not isinstance(data['recovery_reason'], str)
            or not isinstance(fresh, dict) or not {'entropy', 'hedge'} <= fresh.keys()
            or any(type(value) is not bool for value in fresh.values())):
        return 2, 'INVALID: unsupported health schema or field types'
    if pair_id is not None and data['pair_id'] != pair_id:
        return 2, 'INVALID: health belongs to a different pair'
    now = time.time() if now is None else now
    if not _number(now):
        return 2, 'INVALID: current time must be finite'
    age = now - data['ts']
    if not -1 <= age <= max_age:
        return 1, 'STALE: health heartbeat outside allowed age'
    state = data['state']
    if data['stopped'] or state not in {'READY', 'RECORD_ONLY'}:
        reason = (data['halt_reason'] or data['recovery_reason']
                  or data.get('blocked_reason') or 'operator review required')
        return 1, f'{state}: {reason}; pending={data["pending_orders"]}'
    if data['pending_orders'] or not all(fresh.values()):
        return 1, f'{state}: pending orders or stale market books'
    if 'opening_allowed' in data and type(data['opening_allowed']) is not bool:
        return 2, 'INVALID: opening_allowed must be boolean'
    if state == 'READY' and data.get('opening_allowed') is not True:
        return 1, f'PAUSED: {data.get("blocked_reason") or "opening risk gate blocks new exposure"}'
    recorder = data.get('recorder')
    if isinstance(recorder, dict) and recorder.get('healthy') is False:
        return 1, 'DEGRADED: recorder requires operator review'
    return 0, f'{state}: pair={data["pair_id"]} age={max(age, 0):.1f}s net={data["net_base"]:+.8g}'


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('path', help='configured runtime.health_file')
    p.add_argument('--max-age', type=float, default=15.0)
    p.add_argument('--pair-id', help='reject health from a different pair')
    args = p.parse_args(argv)
    code, message = check_health(args.path, max_age=args.max_age, pair_id=args.pair_id)
    print(message)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
