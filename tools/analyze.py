#!/usr/bin/env python3
"""Analyze a single identified pair's observed book opportunities.

Reads the CSV shards written by the built-in recorder (logs/<pair>/minutes-*.csv)
and prints:

  * the premium distribution (midline candidates),
  * minutes in which each candidate band appeared at top of book,
  * exploratory bands once enough identified observations are available.

This is distribution analysis, not a backtest or a profitability guarantee.
Distinct bars sharing one market/minute bucket are excluded with a count; a
clean restart can produce such a partial-minute overlap. Exact bar replays are
deduplicated, while a reused bar ID with changed data is rejected.

Usage:
    python3 tools/analyze.py --csv 'logs/<pair>/minutes-*.csv' --hours 24
    python3 tools/analyze.py --pair <pair-id> --min-samples 10
"""
from __future__ import annotations

import argparse
import csv
import glob
import math
import sys
import time
import warnings

CANDIDATES = [1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0]


def pctl(sorted_vals: list, q: float) -> float:
    """Linear-interpolated percentile of a pre-sorted list, q in [0, 100]."""
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_vals[int(k)]
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


IDENTITY_FIELDS = ("pair_id", "symbol", "entropy_dex", "hedge_venue")


def load_rows(path: str, hours: float, min_samples: int, *,
              allow_legacy: bool = False, pair_id: str | None = None) -> list:
    """Read daily shards; reject ambiguous identities and corrupt observations."""
    if not math.isfinite(hours) or hours < 0:
        raise ValueError("hours must be finite and nonnegative")
    if not 1 <= min_samples <= 120:
        raise ValueError("min_samples must be in [1, 120]")
    paths = sorted(set(glob.glob(path, recursive=True)))
    if not paths:
        raise FileNotFoundError(path)
    cutoff = time.time() - hours * 3600 if hours > 0 else 0.0
    rows = {}
    identities = set()
    legacy_files = 0
    identified_files = 0
    seen_bars = {}
    seen_minutes = set()
    overlapping_minutes = set()
    for filename in paths:
        with open(filename, newline="") as fh:
            reader = csv.DictReader(fh)
            fields = reader.fieldnames or []
            identity_fields = set(IDENTITY_FIELDS).intersection(fields)
            legacy = not identity_fields and "schema_version" not in fields
            if legacy:
                if not allow_legacy:
                    raise ValueError("legacy CSV has no market identity; use --allow-legacy "
                                     "for diagnostic analysis only")
                legacy_files += 1
            elif identity_fields != set(IDENTITY_FIELDS) or "schema_version" not in fields:
                raise ValueError("CSV has incomplete market identity/schema fields")
            else:
                identified_files += 1
            file_identity = None
            for lineno, r in enumerate(reader, start=2):
                if None in r:
                    raise ValueError(f"CSV row {lineno} contains unexpected columns")
                if legacy:
                    identity = ("legacy-unverified", "unknown", "unknown", "unknown")
                    bar_id = f"{filename}:{lineno}"
                else:
                    identity = tuple(r.get(key) for key in IDENTITY_FIELDS)
                    if any(not key for key in identity) or r.get("schema_version") != "2" \
                            or not r.get("bar_id"):
                        raise ValueError(f"CSV row {lineno} has invalid market identity/schema")
                    bar_id = r["bar_id"]
                if file_identity is not None and identity != file_identity:
                    raise ValueError("CSV file contains multiple market identities")
                file_identity = identity
                if pair_id and identity[0] != pair_id:
                    continue
                identities.add(identity)
                try:
                    sample_count = int(r["samples"])
                    observation = {
                        **dict(zip(IDENTITY_FIELDS, identity)),
                        "ts": float(r["minute_ts"]),
                        "prem": float(r["premium_close_bps"]),
                        "prem_mean": float(r["premium_mean_bps"]),
                        "sell_max": float(r["sell_edge_max_bps"]),
                        "buy_max": float(r["buy_edge_max_bps"]),
                    }
                except (KeyError, ValueError, TypeError):
                    raise ValueError(f"CSV row {lineno} contains invalid numeric data") from None
                if not all(math.isfinite(observation[key]) for key in
                           ("ts", "prem", "prem_mean", "sell_max", "buy_max")):
                    raise ValueError(f"CSV row {lineno} numeric data must be finite")
                if not 1 <= sample_count <= 120 or observation["ts"] < 0 \
                        or observation["ts"] % 60:
                    raise ValueError(f"CSV row {lineno} contains invalid timestamp/sample count")
                bar_key = (identity, bar_id)
                fingerprint = tuple(sorted(r.items()))
                if bar_key in seen_bars:
                    if seen_bars[bar_key] != fingerprint:
                        raise ValueError("CSV contains conflicting duplicate bar identifiers")
                    continue
                seen_bars[bar_key] = fingerprint
                minute_key = (identity, observation["ts"])
                if minute_key in seen_minutes:
                    overlapping_minutes.add(minute_key)
                    rows.pop(minute_key, None)
                    continue
                seen_minutes.add(minute_key)
                if observation["ts"] >= cutoff and sample_count >= min_samples:
                    rows[minute_key] = observation
    if legacy_files and identified_files:
        raise ValueError("legacy data cannot be mixed with identified data")
    if legacy_files > 1:
        raise ValueError("legacy data may use only one explicitly selected source file")
    if len(identities) > 1:
        pairs = sorted(identity[0] for identity in identities)
        raise ValueError(f"multiple market identities found: {pairs}; select --pair explicitly")
    if legacy_files:
        warnings.warn("legacy analysis cannot verify the market or exclude prior mixed data; "
                      "do not use it to authorize live trading", UserWarning, stacklevel=2)
    excluded = sum(ts >= cutoff for _, ts in overlapping_minutes)
    if excluded:
        warnings.warn(f"excluded {excluded} overlapping minute bucket(s) from statistics; "
                      "distinct bars may come from a restart or simultaneous recorders", UserWarning,
                      stacklevel=2)
    return sorted(rows.values(), key=lambda r: r["ts"])


def main() -> None:
    p = argparse.ArgumentParser(description="suggest thresholds from recorded "
                                            "minute data")
    p.add_argument("--csv", default="logs/*/minutes-*.csv",
                   help="CSV path or quoted glob of UTC daily shards")
    p.add_argument("--pair", help="explicit pair_id when several pairs match the glob")
    p.add_argument("--allow-legacy", action="store_true",
                   help="diagnostic analysis of one old identity-free CSV; no config suggestion")
    p.add_argument("--hours", type=float, default=0.0,
                   help="only use the last N hours (0 = all data)")
    p.add_argument("--min-samples", type=int, default=10,
                   help="skip minutes with fewer fresh samples than this")
    p.add_argument("--fees-bps", type=float, default=0.0,
                   help="SUM of both venues' taker fees in bps (each crossing "
                        "pays both legs); recorded edges are pre-fee, so this "
                        "is subtracted before counting firings (default 0.0 — "
                        "pass ~1.0 with a tradexyz hedge)")
    args = p.parse_args()
    if not math.isfinite(args.fees_bps) or not 0 <= args.fees_bps < 10000:
        p.error("--fees-bps must be finite and in [0, 10000)")

    try:
        with warnings.catch_warnings(record=True) as notices:
            warnings.simplefilter("always", UserWarning)
            rows = load_rows(args.csv, args.hours, args.min_samples,
                             allow_legacy=args.allow_legacy, pair_id=args.pair)
        for notice in notices:
            print(f"data warning: {notice.message}", file=sys.stderr)
    except FileNotFoundError:
        print(f"{args.csv} not found — run the bot (even --record-only) to "
              f"collect data first", file=sys.stderr)
        sys.exit(1)
    except (ValueError, OSError) as exc:
        print(f"analysis rejected: {exc}", file=sys.stderr)
        sys.exit(1)
    if len(rows) < 30:
        print(f"only {len(rows)} usable minute(s) in {args.csv} — collect at "
              f"least a few hours before trusting the numbers", file=sys.stderr)
        if not rows:
            sys.exit(1)

    span_h = (rows[-1]["ts"] - rows[0]["ts"]) / 3600.0 + 1 / 60.0
    prem = sorted(r["prem"] for r in rows)
    mean = sum(prem) / len(prem)
    var = sum((x - mean) ** 2 for x in prem) / len(prem)
    median = pctl(prem, 50)

    print(f"\n=== {args.csv}: {len(rows)} minutes over {span_h:.1f}h ===\n")
    print(f"pair: {rows[0]['pair_id']} ({rows[0]['symbol']}, "
          f"{rows[0]['entropy_dex']} / {rows[0]['hedge_venue']})")
    print("premium of Entropy over hedge, minute close (bps):")
    print(f"  mean {mean:+.2f}   std {math.sqrt(var):.2f}   "
          f"median {median:+.2f}")
    print(f"  p5 {pctl(prem, 5):+.2f}   p25 {pctl(prem, 25):+.2f}   "
          f"p75 {pctl(prem, 75):+.2f}   p95 {pctl(prem, 95):+.2f}")

    midline = round(median, 1) or 0.0   # normalize -0.0
    # room beyond the midline that was actually executable each minute, net
    # of taker fees (config thresholds are net-of-fee: the engine adds fees
    # on top, and recorded edges are pre-fee)
    fees = args.fees_bps
    sell_room = sorted((r["sell_max"] - midline - fees for r in rows),
                       reverse=True)
    buy_room = sorted((r["buy_max"] + midline - fees for r in rows),
                      reverse=True)

    print(f"\nwith midline_bps = {midline:+.1f} (median) and {fees:.1f} bps "
          "fees per two-leg crossing, observed opportunity minutes:")
    print("One second's top-of-book maximum can qualify a minute. This is not a "
          "backtest: persistence, depth, inventory, funding and fills are not modeled.")
    print(f"  {'band bps':>9} | {'SELL entropy':>17} | {'BUY entropy':>17}")
    print(f"  {'':>9} | {'minutes':>8} {'per day':>8} | "
          f"{'minutes':>8} {'per day':>8}")
    per_day = 24.0 / span_h if span_h > 0 else 0.0
    for t in CANDIDATES:
        s_hits = sum(1 for x in sell_room if x >= t)
        b_hits = sum(1 for x in buy_room if x >= t)
        print(f"  {t:>9.1f} | {s_hits:>8} {s_hits * per_day:>8.1f} | "
              f"{b_hits:>8} {b_hits * per_day:>8.1f}")

    if rows[0]["pair_id"] == "legacy-unverified" or len(rows) < 30:
        print("\nDiagnostic statistics only; no ready-to-use live configuration is emitted.")
        return

    # default suggestion: the band that fired in ~10% of minutes (p90 of the
    # fee-adjusted executable room), floored at 1 bps — tune from the table
    sug_upper = max(round(pctl(sorted(sell_room), 90) * 2) / 2, 1.0)
    sug_lower = max(round(pctl(sorted(buy_room), 90) * 2) / 2, 1.0)
    print(f"""
exploratory starting bands (observed in ~10% of minutes, adjusted for the
{fees:.1f} bps fees passed via --fees-bps; this does not predict execution or profit):

thresholds:
  midline_bps: {midline}
  upper_bps: {sug_upper}
  lower_bps: {sug_lower}

Re-run with --hours to focus on recent regimes; premiums drift, so refresh
these numbers regularly.
""")


if __name__ == "__main__":
    main()
