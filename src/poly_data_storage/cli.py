"""Fetch supported Polygon CLOB OrderFilled history into daily Parquet files.

Usage: poly-fetch --start 2025-01-01 --end 2026-07-31
Dates are inclusive UTC days. Settings remain in constants.py.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import niquests

from poly_data_storage.block_timestamp_resolve import BoundaryCache, resolve_block_range
from poly_data_storage.constants import OUTPUT_DIR, REPLACE
from poly_data_storage.logger import configure_logger, get_logger
from poly_data_storage.sqd_portal import DayNotReady, ScanStats, stream_decoded_logs
from poly_data_storage.storage import (
    ensure_output_dir,
    finish_order_fills,
    is_complete_file,
    open_order_fills_writer,
    write_order_fills_batch,
)

log = get_logger()


def daterange(start: str, end: str) -> list[str]:
    """Return inclusive UTC dates; reject invalid/reversed intervals clearly."""
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=UTC)
    if start_dt > end_dt:
        raise ValueError("--end must be on or after --start")
    dates: list[str] = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def run_for_day(
    client: niquests.Session,
    date_str: str,
    *,
    boundary_cache: BoundaryCache | None = None,
) -> bool | None:
    """Return True for complete, False for failed, or None for not ready."""
    temp_path: Path | None = None
    try:
        out_dir = ensure_output_dir(OUTPUT_DIR)
        filename = f"polymarket_orders_{date_str.replace('-', '_')}.parquet"
        final_path = out_dir / filename
        # Keep the lock inode: unlinking it would allow competing processes to
        # lock different inodes. The OS releases the lock even after a crash.
        with (out_dir / f".{filename}.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(f"Another collector is writing {date_str}") from exc
            if final_path.exists() and not REPLACE:
                if is_complete_file(final_path, date_str):
                    log.info("  Verified existing complete file: %s", final_path)
                    return True
                raise ValueError(
                    f"Existing file is unverified or uses older coverage: {final_path}. "
                    "Set REPLACE=True to explicitly rebuild it, or use a separate OUTPUT_DIR."
                )

            log.info("  %s — collecting all supported exchange fills", date_str)
            t0 = time.monotonic()
            evidence: dict[str, Any] = {}
            scan_start, scan_end, day_start_ts, day_end_ts = resolve_block_range(
                client, date_str, boundary_cache=boundary_cache, evidence=evidence
            )
            fd, name = tempfile.mkstemp(prefix=f".tmp_{filename}.", dir=out_dir)
            os.close(fd)
            temp_path = Path(name)
            stats = ScanStats()
            total_rows = 0
            with open_order_fills_writer(temp_path) as writer:
                with closing(
                    stream_decoded_logs(
                        client,
                        scan_start,
                        scan_end,
                        day_start_ts,
                        day_end_ts,
                        stats=stats,
                    )
                ) as batches:
                    for batch in batches:
                        write_order_fills_batch(writer, batch)
                        total_rows += len(batch)
                finish_order_fills(
                    writer,
                    dict(
                        evidence,
                        **asdict(stats),
                        date=date_str,
                        rows=total_rows,
                    ),
                )
            if not is_complete_file(temp_path, date_str):
                raise ValueError("Written Parquet footer failed completion validation")
            temp_path.replace(final_path)
            log.info(
                "  Saved %s fills to %s in %.1fs",
                f"{total_rows:,}",
                final_path,
                time.monotonic() - t0,
            )
            return True
    except DayNotReady as exc:
        log.warning("  NOT READY for %s: %s", date_str, exc)
        return None
    except Exception:
        log.exception("  FAILED for %s", date_str)
        return False
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="poly-fetch",
        description="Fetch finalized Polymarket CLOB fills across supported versions.",
    )
    parser.add_argument(
        "--start", required=True, help="Start UTC date (YYYY-MM-DD, inclusive)"
    )
    parser.add_argument(
        "--end", required=True, help="End UTC date (YYYY-MM-DD, inclusive)"
    )
    args = parser.parse_args()
    try:
        dates = daterange(args.start, args.end)
    except ValueError as exc:
        parser.error(str(exc))

    configure_logger()
    log.info("  Dates: %s through %s; output: %s", dates[0], dates[-1], OUTPUT_DIR)
    complete = failed = not_ready = 0
    cache: BoundaryCache = {}
    with niquests.Session() as client:
        for date_str in dates:
            result = run_for_day(client, date_str, boundary_cache=cache)
            if result is True:
                complete += 1
            elif result is None:
                not_ready += 1
            else:
                failed += 1
    log.info("  Complete: %s; failed: %s; not ready: %s", complete, failed, not_ready)
    if failed or not_ready:
        sys.exit(1)
