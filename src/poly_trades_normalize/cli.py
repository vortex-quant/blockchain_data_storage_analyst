"""CLI entry point — normalize Polymarket blockchain data into trades and events.

Usage:
    poly-trades-normalize --start 2026-06-01 --end 2026-06-30
    poly-trades-normalize --start 2026-06-21 --end 2026-06-21

All paths and processing settings are defined in config.py.
Only --start and --end are set via CLI.
"""

from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

from poly_trades_normalize import config
from poly_trades_normalize.normalizer import process_day


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


def _process_day_wrapper(date_str: str) -> tuple[bool, str]:
    """Wrapper for ProcessPoolExecutor."""
    return process_day(date_str)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="poly-trades-normalize",
        description=(
            "Normalize Polymarket blockchain orders + events into clean "
            "trades and events parquet files."
        ),
    )
    parser.add_argument(
        "--start", type=str, required=True, help="Start date (YYYY-MM-DD, inclusive)"
    )
    parser.add_argument(
        "--end", type=str, required=True, help="End date (YYYY-MM-DD, inclusive)"
    )
    args = parser.parse_args()

    dates = daterange(args.start, args.end)
    print(f"\n  Dates to process: {len(dates)}")
    print(f"  Range: {dates[0]} to {dates[-1]}")
    print(f"  Orders input:  {config.ORDERS_DIR}")
    print(f"  Events input:  {config.EVENTS_DIR}")
    print(f"  Trades output: {config.TRADES_OUTPUT_DIR}")
    print(f"  Events output: {config.EVENTS_OUTPUT_DIR}")
    print(f"  Max workers:   {config.MAX_WORKERS}")
    print(f"  Replace:       {config.REPLACE}")

    success_count = 0
    fail_count = 0

    with ProcessPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {
            executor.submit(_process_day_wrapper, date_str): date_str
            for date_str in dates
        }

        for future in as_completed(futures):
            date_str = futures[future]
            try:
                ok, msg = future.result()
                print(f"  {msg}")
                if ok:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as exc:
                print(f"  {date_str}: ERROR — {exc}")
                fail_count += 1

    print(f"\n{'=' * 60}")
    print(f"  Complete: {success_count} succeeded, {fail_count} failed")
    print(f"{'=' * 60}")

    if fail_count > 0:
        sys.exit(1)
