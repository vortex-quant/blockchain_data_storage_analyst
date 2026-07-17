"""CLI entry point — fetch Chainlink AnswerUpdated events via SQD Portal.

For each UTC day, one file is produced:
  chainlink_asset_prices_YYYY_MM_DD.parquet — every AnswerUpdated log
  from all tracked Chainlink Data Feed aggregators on Polygon.

Usage:
    chainlink-fetch --start 2026-07-01 --end 2026-07-31
    chainlink-fetch --start 2026-07-09 --end 2026-07-09

All paths and settings are defined in constants.py.
Only --start and --end are set via CLI.
"""

from __future__ import annotations

import sys
import time
from datetime import UTC, datetime, timedelta

import niquests

from chainlink_asset_storage.aggregator_resolve import resolve_aggregators
from chainlink_asset_storage.constants import OUTPUT_DIR, REPLACE
from chainlink_asset_storage.logger import get_logger
from chainlink_asset_storage.sqd_portal import stream_decoded_logs
from chainlink_asset_storage.storage import (
    ensure_output_dir,
    open_asset_prices_writer,
    write_asset_prices_batch,
)
from poly_data_storage.block_timestamp_resolve import resolve_block_range

log = get_logger()


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
    aggregators: dict[str, str],
) -> bool:
    """Fetch and save all AnswerUpdated events for a single UTC day."""
    log.info(f"\n{'=' * 60}")
    log.info(f"  {date_str} — chainlink_asset_prices via SQD Portal")
    log.info(f"{'=' * 60}")

    out_dir = ensure_output_dir(OUTPUT_DIR)
    filename = f"chainlink_asset_prices_{date_str.replace('-', '_')}.parquet"
    final_path = out_dir / filename
    temp_path = out_dir / f".tmp_{filename}"

    if final_path.exists() and not REPLACE:
        log.info(f"  Already exists: {final_path}")
        return True

    if temp_path.exists():
        temp_path.unlink()

    try:
        log.info("\n--- Step 1: Resolving block range ---")
        t0 = time.time()
        scan_start, scan_end, day_start_ts, day_end_ts = resolve_block_range(
            client, date_str
        )
        log.info(f"  Resolved in {time.time() - t0:.1f}s")

        log.info("\n--- Step 2: Fetching & decoding AnswerUpdated logs ---")
        t0 = time.time()
        total_rows = 0
        writer = None

        try:
            for batch in stream_decoded_logs(
                client,
                scan_start,
                scan_end,
                day_start_ts,
                day_end_ts,
                aggregators,
            ):
                if writer is None:
                    writer = open_asset_prices_writer(temp_path)
                write_asset_prices_batch(writer, batch)
                total_rows += len(batch)
        finally:
            if writer is not None:
                writer.close()

        log.info(f"\n  Wrote {total_rows:,} price updates in {time.time() - t0:.1f}s")

        if total_rows == 0:
            w = open_asset_prices_writer(temp_path)
            w.close()
            log.info(f"  No AnswerUpdated events found for {date_str}")

        temp_path.rename(final_path)
        log.info(f"  Saved to {final_path}")

        return True

    except Exception as exc:
        log.info(f"\n  ERROR for {date_str}: {exc}")
        import traceback

        log.info(traceback.format_exc())
        if temp_path.exists():
            temp_path.unlink()
        return False


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="chainlink-fetch",
        description="Fetch Chainlink Data Feed AnswerUpdated events via SQD Portal.",
    )
    parser.add_argument(
        "--start", type=str, required=True, help="Start date (YYYY-MM-DD, inclusive)"
    )
    parser.add_argument(
        "--end", type=str, required=True, help="End date (YYYY-MM-DD, inclusive)"
    )
    args = parser.parse_args()

    dates = daterange(args.start, args.end)

    log.info(f"\n  Dates to fetch: {len(dates)}")
    log.info(f"  Range: {dates[0]} to {dates[-1]}")
    log.info(f"  Output: {OUTPUT_DIR}")

    success_count = 0
    fail_count = 0

    with niquests.Session() as client:
        log.info("\n--- Resolving aggregator addresses ---")
        aggregators = resolve_aggregators(client)

        for i, date_str in enumerate(dates):
            log.info(f"\n{'#' * 60}")
            log.info(f"  Day {i + 1}/{len(dates)}")
            log.info(f"{'#' * 60}")

            ok = run_for_day(client, date_str, aggregators)
            if ok:
                success_count += 1
            else:
                fail_count += 1

    log.info(f"\n{'=' * 60}")
    log.info(f"  Complete: {success_count} succeeded, {fail_count} failed")
    log.info(f"{'=' * 60}")

    if fail_count > 0:
        sys.exit(1)
