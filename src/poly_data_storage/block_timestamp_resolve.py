"""Block range resolution via SQD Portal timestamp endpoint."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import niquests

from poly_data_storage.constants import (
    MAX_RETRIES,
    RETRY_BASE_DELAY,
    SQD_TIMESTAMP_URL,
)
from poly_data_storage.logger import get_logger

log = get_logger()


def _sqd_timestamp_to_block(client: niquests.Session, target_ts: int) -> int:
    """Resolve a Unix timestamp to an exact block number via SQD Portal.

    SQD Portal indexes full Polygon history from genesis to head.
    Returns the block number whose timestamp is at or after target_ts.
    """
    url = SQD_TIMESTAMP_URL.format(ts=target_ts)
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(url, timeout=30.0)
            if resp.status_code == 404:
                raise ValueError(f"No block found at or after timestamp {target_ts}")
            resp.raise_for_status()
            return resp.json()["block_number"]
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                log.info(
                    f"  SQD timestamp lookup retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}"
                )
                time.sleep(delay)
            else:
                raise


def resolve_block_range(
    client: niquests.Session,
    date_str: str,
) -> tuple[int, int, int, int]:
    """Resolve block range covering a UTC day via SQD Portal timestamp endpoint.

    Returns (start_block, end_block, day_start_ts, day_end_ts).
    """
    day_start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end_dt = day_start_dt + timedelta(days=1)
    day_start_ts = int(day_start_dt.timestamp())
    day_end_ts = int(day_end_dt.timestamp())

    log.info("  Resolving block range via SQD Portal timestamp endpoint...")
    start_block = _sqd_timestamp_to_block(client, day_start_ts)
    end_block = _sqd_timestamp_to_block(client, day_end_ts)
    log.info(f"  UTC day: {day_start_dt.isoformat()} -> {day_end_dt.isoformat()}")
    log.info(
        f"  Block range: {start_block} to {end_block} ({end_block - start_block} blocks)"
    )

    return start_block, end_block, day_start_ts, day_end_ts
