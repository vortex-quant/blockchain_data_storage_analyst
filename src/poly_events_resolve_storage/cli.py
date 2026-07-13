"""CLI — fetch Polymarket event resolutions from UMA oracle on Polygon.

For each UTC day, one file is produced:
  polymarket_events_resolve_YYYY_MM_DD.parquet — one row per resolved market

Usage:
    poly-events-resolve 2026-07-12
    poly-events-resolve --start 2026-07-01 --end 2026-07-31
    poly-events-resolve 2026-07-12 --output /data --replace
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import niquests

from poly_events_resolve_storage.constants import OUTPUT_DIR
from poly_events_resolve_storage.sqd_portal import fetch_events, resolve_block_range
from poly_events_resolve_storage.storage import write_parquet


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
    replace: bool = False,
) -> bool:
    """Fetch and save all UMA event resolutions for a single UTC day."""
    print(f"\n{'=' * 60}")
    print(f"  {date_str} — polymarket_events_resolve")
    print(f"{'=' * 60}")

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"polymarket_events_resolve_{date_str.replace('-', '_')}.parquet"
    final_path = output_dir / filename
    temp_path = output_dir / f".tmp_{filename}"

    if final_path.exists() and not replace:
        print(f"  Already exists: {final_path} (use --replace to overwrite)")
        return True

    if temp_path.exists():
        temp_path.unlink()

    try:
        t0 = time.time()
        start_block, end_block, day_start_ts, day_end_ts = resolve_block_range(
            client, date_str
        )
        rows = fetch_events(client, start_block, end_block, day_start_ts, day_end_ts)
        print(f"  Fetched {len(rows)} resolutions in {time.time() - t0:.1f}s")

        write_parquet(rows, temp_path)
        temp_path.rename(final_path)
        print(f"  Saved to {final_path}")
        return True

    except Exception as exc:
        print(f"\n  ERROR for {date_str}: {exc}")
        import traceback

        traceback.print_exc()
        if temp_path.exists():
            temp_path.unlink()
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="poly-events-resolve",
        description="Fetch Polymarket event resolutions from UMA oracle on Polygon.",
    )
    parser.add_argument(
        "date",
        nargs="?",
        default=None,
        help="Single date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--start", type=str, default=None, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=str, default=None, help="End date (YYYY-MM-DD, inclusive)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--replace", action="store_true", help="Overwrite existing files"
    )
    args = parser.parse_args()

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

            ok = run_for_day(client, date_str, output_dir, replace=args.replace)
            if ok:
                success_count += 1
            else:
                fail_count += 1

    print(f"\n{'=' * 60}")
    print(f"  Complete: {success_count} succeeded, {fail_count} failed")
    print(f"  Output: {output_dir}")
    print(f"{'=' * 60}")

    if fail_count > 0:
        sys.exit(1)
