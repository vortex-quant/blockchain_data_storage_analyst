"""CLI entry point — fetch Polymarket crypto events from Gamma API.

For each UTC day, one file is produced:
  polymarket_events_YYYY_MM_DD.parquet — all crypto event metadata

Usage:
    poly-events --start 2026-07-01 --end 2026-07-31
    poly-events --start 2026-07-09 --end 2026-07-09 --output /data/polymarket
    poly-events 2026-07-09
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import niquests

from poly_events_storage.constants import OUTPUT_DIR
from poly_events_storage.gamma_api import fetch_crypto_events_for_day
from poly_events_storage.storage import ensure_output_dir, open_events_writer, write_events_batch


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
    """Fetch and save all crypto events for a single UTC day.

    Returns True on success, False on failure.
    """
    print(f"\n{'=' * 60}")
    print(f"  {date_str} — polymarket_events via Gamma API")
    print(f"{'=' * 60}")

    out_dir = ensure_output_dir(output_dir)
    filename = f"polymarket_events_{date_str.replace('-', '_')}.parquet"
    final_path = out_dir / filename
    temp_path = out_dir / f".tmp_{filename}"

    if final_path.exists() and not replace:
        print(f"  Already exists: {final_path} (use --replace to overwrite)")
        return True

    if temp_path.exists():
        print(f"  Cleaning up incomplete temp file from previous run")
        temp_path.unlink()

    try:
        # Step 1: Fetch all crypto events for the UTC day
        print(f"\n--- Step 1: Fetching crypto events from Gamma API ---")
        t0 = time.time()
        events, failures = fetch_crypto_events_for_day(client, date_str)
        print(f"  Fetched {len(events)} events in {time.time() - t0:.1f}s")

        if failures:
            for f in failures:
                print(f"  WARNING: {f}")

        # Step 2: Write to parquet
        print(f"\n--- Step 2: Writing parquet ---")
        writer = open_events_writer(temp_path)
        total_rows = 0
        try:
            if events:
                total_rows = write_events_batch(writer, events)
        finally:
            writer.close()

        print(f"  Wrote {total_rows} rows ({len(events)} events)")

        # Step 3: Atomic rename
        if not events:
            print(f"  No crypto events found for {date_str}")

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
        prog="poly-events",
        description="Fetch Polymarket crypto event metadata from Gamma API.",
    )
    parser.add_argument(
        "date",
        nargs="?",
        default=None,
        help="Single date in YYYY-MM-DD format",
    )
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD, inclusive)")
    parser.add_argument("--output", type=str, default=None, help=f"Output directory (default: {OUTPUT_DIR})")
    parser.add_argument("--replace", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    if args.start and args.end:
        dates = daterange(args.start, args.end)
    elif args.date:
        dates = [args.date]
    else:
        parser.error("Provide --start and --end (or a single date argument)")

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
