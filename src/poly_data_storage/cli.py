"""CLI entry point — fetch Polymarket OrderFilled V2 events via SQD Portal.

For each UTC day, one file is produced:
  polymarket_orders_YYYY_MM_DD.parquet — every OrderFilled log (raw blockchain facts)

Usage:
    poly-fetch 2026-07-09
    poly-fetch --start 2026-07-01 --end 2026-07-31
    poly-fetch --start 2026-07-01 --end 2026-07-31 --output /data/polymarket
    poly-fetch 2026-07-09 --replace
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import niquests

from poly_data_storage.block_utils import estimate_block_range
from poly_data_storage.constants import OUTPUT_DIR
from poly_data_storage.logger import get_logger
from poly_data_storage.sqd_portal import stream_decoded_logs
from poly_data_storage.storage import (
    ensure_output_dir,
    open_order_fills_writer,
    write_order_fills_batch,
)

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
    output_dir: Path,
    replace: bool = False,
) -> bool:
    """Fetch and save all OrderFilled V2 events for a single UTC day.

    Returns True on success, False on failure.
    """
    log.info(f"\n{'=' * 60}")
    log.info(f"  {date_str} — polymarket_orders via SQD Portal")
    log.info(f"{'=' * 60}")

    out_dir = ensure_output_dir(output_dir)
    filename = f"polymarket_orders_{date_str.replace('-', '_')}.parquet"
    final_path = out_dir / filename
    temp_path = out_dir / f".tmp_{filename}"

    if final_path.exists() and not replace:
        log.info(f"  Already exists: {final_path} (use --replace to overwrite)")
        return True

    if temp_path.exists():
        log.info("  Cleaning up incomplete temp file from previous run")
        temp_path.unlink()

    try:
        # Step 1: Estimate block range from UTC midnight boundaries
        log.info("\n--- Step 1: Resolving block range ---")
        t0 = time.time()
        scan_start, scan_end, day_start_ts, day_end_ts = estimate_block_range(
            client, date_str
        )
        log.info(f"  Resolved in {time.time() - t0:.1f}s")

        # Step 2: Stream + decode + filter + write incrementally
        log.info("\n--- Step 2: Fetching & decoding OrderFilled logs ---")
        t0 = time.time()
        total_rows = 0
        writer = None

        try:
            for batch in stream_decoded_logs(
                client, scan_start, scan_end, day_start_ts, day_end_ts
            ):
                if writer is None:
                    writer = open_order_fills_writer(temp_path)
                write_order_fills_batch(writer, batch)
                total_rows += len(batch)
        finally:
            if writer is not None:
                writer.close()

        log.info(f"\n  Wrote {total_rows:,} order fills in {time.time() - t0:.1f}s")

        # Step 3: Atomic rename — create empty file if no fills found
        if total_rows == 0:
            w = open_order_fills_writer(temp_path)
            w.close()
            log.info(f"  No order fills found for {date_str}")

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
    parser = argparse.ArgumentParser(
        prog="poly-fetch",
        description="Fetch Polymarket OrderFilled V2 events via SQD Portal.",
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

    log.info(f"\n  Dates to fetch: {len(dates)}")
    log.info(f"  Output directory: {output_dir}")
    if len(dates) > 1:
        log.info(f"  Range: {dates[0]} to {dates[-1]}")

    success_count = 0
    fail_count = 0

    with niquests.Session() as client:
        for i, date_str in enumerate(dates):
            log.info(f"\n{'#' * 60}")
            log.info(f"  Day {i + 1}/{len(dates)}")
            log.info(f"{'#' * 60}")

            ok = run_for_day(client, date_str, output_dir, replace=args.replace)
            if ok:
                success_count += 1
            else:
                fail_count += 1

    log.info(f"\n{'=' * 60}")
    log.info(f"  Complete: {success_count} succeeded, {fail_count} failed")
    log.info(f"  Output: {output_dir}")
    log.info(f"{'=' * 60}")

    if fail_count > 0:
        sys.exit(1)
