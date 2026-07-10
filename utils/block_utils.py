"""Polymarket SQD Portal data fetcher — block estimation helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import niquests

from utils.constants import BLOCK_BUFFER, POLYGON_BLOCK_TIME, POLYGON_RPC_URL


def find_block_for_timestamp(
    target_ts: int,
    known_block: int,
    known_block_ts: int,
) -> int:
    """Estimate block number for a target Unix timestamp using ~2s block time."""
    delta_seconds = target_ts - known_block_ts
    delta_blocks = int(delta_seconds / POLYGON_BLOCK_TIME)
    return known_block + delta_blocks


def get_latest_block_info(client: niquests.Session) -> tuple[int, int]:
    """Get latest block number and timestamp from Polygon RPC."""
    resp = client.post(
        POLYGON_RPC_URL,
        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
        timeout=30.0,
    )
    resp.raise_for_status()
    block_hex = resp.json()["result"]
    block_num = int(block_hex, 16)

    resp = client.post(
        POLYGON_RPC_URL,
        json={"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [block_hex, False], "id": 2},
        timeout=30.0,
    )
    resp.raise_for_status()
    block_ts = int(resp.json()["result"]["timestamp"], 16)

    return block_num, block_ts


def estimate_block_range(
    client: niquests.Session,
    date_str: str,
) -> tuple[int, int, int, int]:
    """Estimate block range covering a UTC day.

    Uses UTC midnight boundaries calibrated against the latest block from
    Polygon RPC. Returns (start_block, end_block, day_start_ts, day_end_ts).
    The caller must filter by timestamp [day_start_ts, day_end_ts) before
    writing, as the block range is a deliberately wide superset.
    """
    day_start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end_dt = day_start_dt + timedelta(days=1)
    day_start_ts = int(day_start_dt.timestamp())
    day_end_ts = int(day_end_dt.timestamp())

    latest_block, latest_ts = get_latest_block_info(client)
    print(f"  Latest block: {latest_block} (ts: {datetime.fromtimestamp(latest_ts, tz=UTC)})")

    est_start = find_block_for_timestamp(day_start_ts, latest_block, latest_ts)
    est_end = find_block_for_timestamp(day_end_ts, latest_block, latest_ts)

    scan_start = max(0, est_start - BLOCK_BUFFER)
    scan_end = est_end + BLOCK_BUFFER

    print(f"  UTC day: {day_start_dt.isoformat()} -> {day_end_dt.isoformat()}")
    print(f"  Block range: {scan_start} to {scan_end} ({scan_end - scan_start} blocks)")

    return scan_start, scan_end, day_start_ts, day_end_ts
