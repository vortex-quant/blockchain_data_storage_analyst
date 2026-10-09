"""Exact UTC boundaries verified against finalized SQD block headers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import niquests

from poly_data_storage.constants import SQD_FINALIZED_HEAD_URL, SQD_TIMESTAMP_URL
from poly_data_storage.logger import get_logger
from poly_data_storage.sqd_portal import DayNotReady, get_json, stream_blocks

log = get_logger()
BoundaryCache = dict[int, tuple[int, str]]


def _sqd_timestamp_to_block(client: niquests.Session, target_ts: int) -> int:
    result = get_json(
        client, SQD_TIMESTAMP_URL.format(ts=target_ts), timestamp_lookup=True
    )
    number = result.get("block_number") if isinstance(result, dict) else None
    if type(number) is not int or number < 0:
        raise ValueError("SQD timestamp lookup returned an invalid block number")
    return number


def _verify_boundary(client: niquests.Session, target_ts: int, number: int) -> str:
    blocks = list(stream_blocks(client, max(0, number - 1), number, include_logs=False))
    header = blocks[-1]["header"]
    if header["timestamp"] < target_ts or (
        number > 0 and blocks[0]["header"]["timestamp"] >= target_ts
    ):
        raise ValueError(
            f"SQD block {number} is not the first block at or after {target_ts}"
        )
    block_hash = header.get("hash")
    if not isinstance(block_hash, str) or len(block_hash) != 66:
        raise ValueError(f"Missing block hash for verified boundary {number}")
    return block_hash


def resolve_block_range(
    client: niquests.Session,
    date_str: str,
    *,
    boundary_cache: BoundaryCache | None = None,
    evidence: dict[str, Any] | None = None,
) -> tuple[int, int, int, int]:
    """Return inclusive scan bounds and a half-open UTC timestamp interval.

    The next day's boundary itself must be finalized before publishing a day.
    Equal boundary numbers mean an exact empty day, not an estimated range.
    """
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    if day_end > datetime.now(UTC):
        raise DayNotReady(f"UTC day {date_str} has not ended yet")
    start_ts, end_ts = int(day_start.timestamp()), int(day_end.timestamp())
    cache = boundary_cache if boundary_cache is not None else {}
    start_block = (
        cache[start_ts][0]
        if start_ts in cache
        else _sqd_timestamp_to_block(client, start_ts)
    )
    next_block = (
        cache[end_ts][0] if end_ts in cache else _sqd_timestamp_to_block(client, end_ts)
    )
    if next_block < start_block:
        raise ValueError("SQD returned reversed daily block boundaries")
    head = get_json(client, SQD_FINALIZED_HEAD_URL)
    if not isinstance(head, dict) or type(head.get("number")) is not int:
        raise DayNotReady("SQD has no finalized head available")
    if next_block > head["number"]:
        raise DayNotReady(
            f"Boundary block {next_block} is beyond finalized head {head['number']}"
        )
    for ts, number in ((start_ts, start_block), (end_ts, next_block)):
        if ts not in cache:
            cache[ts] = number, _verify_boundary(client, ts, number)
    if evidence is not None:
        evidence.update(
            day_start_ts=start_ts,
            day_end_ts=end_ts,
            start_block=start_block,
            end_block=next_block - 1,
            next_day_block=next_block,
            start_block_hash=cache[start_ts][1],
            next_day_block_hash=cache[end_ts][1],
            finalized_head=head["number"],
            finalized_head_hash=head.get("hash"),
        )
    log.info(
        "  Verified UTC day %s: finalized blocks %s-%s",
        date_str,
        start_block,
        next_block - 1,
    )
    return start_block, next_block - 1, start_ts, end_ts
