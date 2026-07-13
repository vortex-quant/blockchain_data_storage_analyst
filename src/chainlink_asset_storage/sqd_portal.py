"""SQD Portal log fetching and AnswerUpdated event decoding.

AnswerUpdated event from Chainlink aggregator contracts:
    event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)

Log layout:
    topics[0] = event signature hash
    topics[1] = current (int256 — price answer, 8 decimals for USD feeds)
    topics[2] = roundId (uint256)
    data      = updatedAt (uint256 — Unix timestamp)
"""

from __future__ import annotations

import time
from collections.abc import Generator
from datetime import UTC, datetime

import niquests
import orjson

from chainlink_asset_storage.constants import (
    ANSWER_UPDATED_TOPIC,
    FEED_DECIMALS,
    HEARTBEAT_SECONDS,
    RETRY_BASE_DELAY,
    SQD_DELAY,
    SQD_MAX_BLOCKS_PER_REQUEST,
    SQD_URL,
    WRITE_BATCH_SIZE,
)
from chainlink_asset_storage.logger import get_logger

log = get_logger()


def decode_answer_updated(
    log_entry: dict,
    block_num: int,
    block_ts: int,
    asset: str,
    aggregator: str,
) -> dict | None:
    """Decode an AnswerUpdated event into a price record.

    The output schema mirrors Chainlink Data Streams v3 fields where possible:
      - price: converted from 8 decimals to float USD
      - valid_from_timestamp / observations_timestamp: both = updatedAt
      - expires_at: updatedAt + heartbeat
      - round_id: from the event
      - bid/ask: omitted (not available in Data Feeds)
    """
    topics = log_entry.get("topics", [])
    if len(topics) < 3:
        return None

    # topic1 = current (int256, two's complement)
    current_raw = int(topics[1], 16)
    if current_raw >= 2**255:
        current_raw -= 2**256

    # topic2 = roundId (uint256)
    round_id = int(topics[2], 16)

    # data = updatedAt (uint256, first 32 bytes)
    data = log_entry.get("data", "")
    hex_data = data[2:] if data.startswith("0x") else data
    if len(hex_data) < 64:
        return None
    updated_at = int(hex_data[:64], 16)

    price_usd = current_raw / (10**FEED_DECIMALS)

    return {
        "block_number": block_num,
        "timestamp": block_ts,
        "block_time": datetime.fromtimestamp(block_ts, tz=UTC).isoformat(),
        "tx_hash": log_entry.get("transactionHash"),
        "log_index": log_entry.get("logIndex"),
        "asset": asset,
        "aggregator_address": aggregator,
        "round_id": round_id,
        "valid_from_timestamp": updated_at,
        "observations_timestamp": updated_at,
        "expires_at": updated_at + HEARTBEAT_SECONDS,
        "price": price_usd,
    }


def _fetch_sqd_raw(
    client: niquests.Session,
    from_block: int,
    to_block: int,
    aggregators: dict[str, str],
) -> Generator[tuple[dict, int, int, str, str], None, None]:
    """Stream logs from SQD Portal for all aggregator addresses.

    Yields (log_dict, block_num, block_ts, asset, aggregator_address).
    """
    # Build address → asset mapping (lowercase for matching)
    addr_to_asset: dict[str, tuple[str, str]] = {}
    for asset, agg in aggregators.items():
        addr_to_asset[agg.lower()] = (asset, agg)

    current = from_block
    retries = 0
    max_retries = 5
    parse_failures = 0

    while current <= to_block:
        payload = {
            "type": "evm",
            "fromBlock": current,
            "toBlock": to_block,
            "fields": {
                "block": {"number": True, "timestamp": True},
                "log": {
                    "address": True,
                    "topics": True,
                    "data": True,
                    "transactionHash": True,
                    "logIndex": True,
                },
            },
            "logs": [
                {
                    "address": list(aggregators.values()),
                    "topic0": [ANSWER_UPDATED_TOPIC],
                }
            ],
        }

        block_count = to_block - current + 1
        resp = None
        try:
            log.info(
                f"  Requesting blocks {current}-{to_block} ({block_count} blocks)..."
            )
            resp = client.post(SQD_URL, json=payload, timeout=120.0, stream=True)
            resp.raise_for_status()
            retries = 0
            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                except Exception:
                    parse_failures += 1
                    continue
                if "header" not in obj:
                    continue
                block_num = obj["header"]["number"]
                block_ts = obj["header"]["timestamp"]
                current = block_num + 1
                for entry in obj.get("logs", []):
                    addr = entry.get("address", "").lower()
                    match = addr_to_asset.get(addr)
                    if match is None:
                        continue
                    asset, agg = match
                    yield (entry, block_num, block_ts, asset, agg)
            resp.close()
            break
        except Exception as exc:
            if resp is not None:
                try:
                    resp.close()
                except Exception:
                    pass
            retries += 1
            if retries > max_retries:
                raise
            delay = RETRY_BASE_DELAY * (2 ** (retries - 1))
            log.info(
                f"      SQD stream reset at block {current}, "
                f"retry {retries}/{max_retries} after {delay}s: {exc}"
            )
            time.sleep(delay)

    if parse_failures:
        log.info(f"  WARNING: {parse_failures} unparseable lines in this range")


def stream_decoded_logs(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    day_start_ts: int,
    day_end_ts: int,
    aggregators: dict[str, str],
    batch_size: int = WRITE_BATCH_SIZE,
) -> Generator[list[dict], None, None]:
    """Fetch and decode AnswerUpdated logs, yielding batches of filtered rows.

    - Streams from SQD Portal with automatic continuation on stream resets.
    - Filters by timestamp [day_start_ts, day_end_ts) to enforce exact UTC day.
    - Deduplicates by (tx_hash, log_index) per page (bounded memory).
    - Yields batches of up to batch_size decoded rows for incremental writing.
    """
    seen: set[tuple[str, int]] = set()
    batch: list[dict] = []
    current = start_block
    page = 0
    total_decoded = 0

    while current <= end_block:
        to_block = min(current + SQD_MAX_BLOCKS_PER_REQUEST - 1, end_block)
        page += 1
        page_decoded = 0
        page_min_ts: int | None = None
        page_max_ts: int | None = None
        last_block = current - 1

        for raw_log, block_num, block_ts, asset, agg in _fetch_sqd_raw(
            client, current, to_block, aggregators
        ):
            last_block = block_num
            if page_min_ts is None or block_ts < page_min_ts:
                page_min_ts = block_ts
            if page_max_ts is None or block_ts > page_max_ts:
                page_max_ts = block_ts

            if block_ts < day_start_ts or block_ts >= day_end_ts:
                continue

            record = decode_answer_updated(raw_log, block_num, block_ts, asset, agg)
            if record is None:
                continue

            key = (record["tx_hash"], record["log_index"])
            if key in seen:
                continue
            seen.add(key)

            batch.append(record)
            page_decoded += 1
            total_decoded += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if page_min_ts is not None:
            from_str = datetime.fromtimestamp(page_min_ts, tz=UTC).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            to_str = datetime.fromtimestamp(page_max_ts, tz=UTC).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            log.info(
                f"  [page {page}] blocks {current}-{to_block}: {page_decoded} updates "
                f"({from_str} to {to_str}, total: {total_decoded:,})"
            )
        else:
            log.info(f"  [page {page}] blocks {current}-{to_block}: 0 logs")

        seen.clear()
        current = last_block + 1 if last_block >= current else to_block + 1
        time.sleep(SQD_DELAY)

    if batch:
        yield batch
