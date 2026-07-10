"""Fetch Polymarket trade data for one or more days using SQD Portal.

SQD Portal is a free, no-API-key blockchain data service that streams EVM logs.
This script fetches ALL OrderFilled V2 events from the Polymarket CTF Exchange
contract, decodes them, and saves to compressed parquet files.

For each day, two files are produced:
  - polymarket_events_YYYY_MM_DD.parquet  — event metadata from Gamma API
  - polymarket_trades_YYYY_MM_DD.parquet  — all decoded trades from SQD Portal

Usage:
    uv run python project01_polymarket_db_store.py 2026-07-09
    uv run python project01_polymarket_db_store.py --start 2026-07-01 --end 2026-07-31
    uv run python project01_polymarket_db_store.py --start 2026-07-01 --end 2026-07-31 --output /data/polymarket
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import niquests

from utils.constants import OUTPUT_DIR, SLUG_PREFIXES
from utils.gamma_api import fetch_all_events_for_day, extract_token_maps
from utils.sqd_portal import fetch_all_sqd_logs, decode_all_logs
from utils.block_utils import estimate_block_range
from utils.storage import save_trades, save_events


def daterange(start: str, end: str) -> list[str]:
    """Generate list of YYYY-MM-DD strings from start to end inclusive."""
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=UTC)
    dates: list[str] = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def run_for_day(
    client: niquests.Session,
    date_str: str,
    output_dir: Path,
) -> bool:
    """Fetch and save all trades + events for a single day.

    Returns True on success, False on failure.
    """
    print(f"\n{'=' * 60}")
    print(f"  {date_str} — Polymarket trades via SQD Portal")
    print(f"{'=' * 60}")

    try:
        # Step 1: Fetch event metadata from Gamma API
        print(f"\n--- Step 1: Fetching events from Gamma API ---")
        t0 = time.time()
        events = fetch_all_events_for_day(client, date_str, SLUG_PREFIXES)
        t1 = time.time()
        print(f"\n  Found {len(events)} events in {t1 - t0:.1f}s")

        if not events:
            print("  No events found! Skipping day.")
            return False

        print(f"  First: ID={events[0].get('id')}, slug={events[0].get('slug')}")
        print(f"  Last:  ID={events[-1].get('id')}, slug={events[-1].get('slug')}")

        # Save events
        save_events(events, date_str, output_dir)

        # Extract token ID mappings
        token_to_event, token_to_outcome = extract_token_maps(events)
        print(f"  Extracted {len(token_to_event)} token IDs")

        # Step 2: Estimate block range
        print(f"\n--- Step 2: Estimating block range ---")
        scan_start, scan_end = estimate_block_range(client, events, date_str)

        # Step 3: Fetch ALL OrderFilled logs from SQD Portal
        print(f"\n--- Step 3: Fetching OrderFilled logs from SQD Portal ---")
        t0 = time.time()
        raw_logs = fetch_all_sqd_logs(client, scan_start, scan_end)
        t1 = time.time()
        print(f"\n  Fetched {len(raw_logs):,} logs in {t1 - t0:.1f}s")

        # Step 4: Decode all logs
        print(f"\n--- Step 4: Decoding logs ---")
        t0 = time.time()
        trades = decode_all_logs(raw_logs, token_to_event, token_to_outcome)
        t1 = time.time()

        matched = sum(1 for t in trades if t.get("_event_id") is not None)
        print(f"  Decoded {len(trades):,} trades in {t1 - t0:.1f}s")
        print(f"  Matched to events: {matched:,}")
        print(f"  Unmatched: {len(trades) - matched:,}")

        # Step 5: Save trades
        print(f"\n--- Step 5: Saving trades ---")
        save_trades(trades, date_str, output_dir)

        # Summary
        print(f"\n--- Summary ---")
        print(f"  Events: {len(events)}")
        print(f"  Trades: {len(trades):,}")
        print(f"  Matched: {matched:,} ({matched / len(trades) * 100:.1f}%)" if trades else "  No trades")
        print(f"  Data source: SQD Portal (free, no API key)")

        return True

    except Exception as exc:
        print(f"\n  ERROR for {date_str}: {exc}")
        import traceback
        traceback.print_exc()
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Polymarket trades via SQD Portal (free, no API key)."
    )
    parser.add_argument(
        "date",
        nargs="?",
        default=None,
        help="Single date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date for range (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date for range (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    # Determine dates to fetch
    if args.start and args.end:
        dates = daterange(args.start, args.end)
    elif args.date:
        dates = [args.date]
    else:
        parser.error("Provide a date argument or --start/--end for a range")

    output_dir = Path(args.output) if args.output else OUTPUT_DIR

    print(f"\n  Dates to fetch: {len(dates)}")
    print(f"  Output directory: {output_dir}")
    if len(dates) > 1:
        print(f"  Range: {dates[0]} to {dates[-1]}")

    success_count = 0
    fail_count = 0

    with niquests.Session() as client:
        for i, date_str in enumerate(dates):
            print(f"\n{'#' * 60}")
            print(f"  Day {i + 1}/{len(dates)}")
            print(f"{'#' * 60}")

            ok = run_for_day(client, date_str, output_dir)
            if ok:
                success_count += 1
            else:
                fail_count += 1

    print(f"\n{'=' * 60}")
    print(f"  Complete: {success_count} succeeded, {fail_count} failed")
    print(f"  Output: {output_dir}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
