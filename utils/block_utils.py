"""Polymarket SQD Portal data fetcher — block estimation helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import niquests

from utils.constants import BLOCK_BUFFER, POLYGON_BLOCK_TIME, TIME_BUFFER_SECONDS
from utils.sqd_portal import fetch_sqd_page


def find_block_for_timestamp(
    target_ts: int,
    known_block: int,
    known_block_ts: int,
) -> int:
    """Estimate block number for a target Unix timestamp using ~1.5s block time."""
    delta_seconds = target_ts - known_block_ts
    delta_blocks = int(delta_seconds / POLYGON_BLOCK_TIME)
    return known_block + delta_blocks


def get_latest_block_info(client: niquests.Session) -> tuple[int, int]:
    """Get latest block number and timestamp from SQD Portal."""
    logs = fetch_sqd_page(client, 0, 0)
    if logs:
        return logs[-1][1], logs[-1][2]
    return 89_885_600, 1_783_533_395


def estimate_block_range(
    client: niquests.Session,
    events: list[dict],
    fallback_date_str: str,
) -> tuple[int, int]:
    """Estimate the block range covering all events for a day.

    Uses event start/end times with buffers, calibrated against the latest
    block from SQD Portal.

    Returns (start_block, end_block).
    """
    latest_block, latest_ts = get_latest_block_info(client)
    print(f"  Latest known block: {latest_block} (ts: {datetime.fromtimestamp(latest_ts, tz=UTC)})")

    start_times: list[int] = []
    end_times: list[int] = []

    for ev in events:
        st = ev.get("startTime") or ev.get("startDate")
        et = ev.get("endDate") or ev.get("closedTime")
        if st:
            ts = st.replace("Z", "+00:00") if isinstance(st, str) else st
            start_times.append(int(datetime.fromisoformat(ts).timestamp()))
        if et:
            ts = et.replace("Z", "+00:00") if isinstance(et, str) else et
            end_times.append(int(datetime.fromisoformat(ts).timestamp()))

    if start_times and end_times:
        scan_ts_start = min(start_times) - TIME_BUFFER_SECONDS
        scan_ts_end = max(end_times) + TIME_BUFFER_SECONDS
    else:
        day_start = int(
            datetime.strptime(fallback_date_str, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        )
        scan_ts_start = day_start
        scan_ts_end = day_start + 86_400

    est_start = find_block_for_timestamp(scan_ts_start, latest_block, latest_ts)
    est_end = find_block_for_timestamp(scan_ts_end, latest_block, latest_ts)

    scan_start = max(0, est_start - BLOCK_BUFFER)
    scan_end = est_end + BLOCK_BUFFER

    print(f"  Scan window: {datetime.fromtimestamp(scan_ts_start, tz=UTC)} -> {datetime.fromtimestamp(scan_ts_end, tz=UTC)}")
    print(f"  Block range: {scan_start} to {scan_end} ({scan_end - scan_start} blocks)")

    return scan_start, scan_end
