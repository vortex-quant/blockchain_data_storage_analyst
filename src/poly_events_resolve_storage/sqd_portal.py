"""SQD Portal — block resolution, log fetching, and ConditionResolution decoding.

Fetches ConditionResolution events from the Gnosis ConditionalTokens contract
on Polygon. This single contract emits resolution events for ALL Polymarket
markets — both standard (UmaCtfAdapter) and NegRisk.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import niquests
import orjson

from poly_events_resolve_storage.constants import (
    CONDITION_RESOLUTION_TOPIC,
    CTF_ADDRESS,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
    SQD_DELAY,
    SQD_MAX_BLOCKS_PER_REQUEST,
    SQD_TIMESTAMP_URL,
    SQD_URL,
)

# ── Block resolution ──────────────────────────────────────────────────────────


def _resolve_block(client: niquests.Session, target_ts: int) -> int:
    """Resolve a Unix timestamp to block number via SQD Portal."""
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
                time.sleep(RETRY_BASE_DELAY * (2**attempt))
            else:
                raise exc


def resolve_block_range(
    client: niquests.Session,
    date_str: str,
) -> tuple[int, int, int, int]:
    """Resolve block range covering a UTC day. Returns (start, end, ts_start, ts_end)."""
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    ts_start = int(day_start.timestamp())
    ts_end = int(day_end.timestamp())

    print("  Resolving block range via SQD Portal...")
    start_block = _resolve_block(client, ts_start)
    end_block = _resolve_block(client, ts_end)
    print(f"  {day_start.isoformat()} -> {day_end.isoformat()}")
    print(f"  Blocks {start_block}-{end_block} ({end_block - start_block} blocks)")

    return start_block, end_block, ts_start, ts_end


# ── Event decoding ────────────────────────────────────────────────────────────


def _decode_resolution(log: dict, block_num: int, block_ts: int) -> dict | None:
    """Decode ConditionResolution event.

    ConditionResolution(bytes32 indexed conditionId, address indexed oracle,
        bytes32 indexed questionId, uint256 outcomeSlotCount, uint256[] payoutNumerators)

    For binary markets: payouts [1,0] = Up/Yes, [0,1] = Down/No.
    settled_price: 1 = Up, 0 = Down.
    """
    topics = log.get("topics", [])
    if len(topics) < 4:
        return None
    data = log.get("data", "")
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 128:
        return None
    # data: outcomeSlotCount (uint256), payoutNumerators (dynamic array)
    # Dynamic array offset (in bytes)
    offset = int(data[64:128], 16) * 2
    if offset + 64 > len(data):
        return None
    arr_length = int(data[offset : offset + 64], 16)
    payouts: list[int] = []
    for i in range(arr_length):
        start = offset + 64 + i * 64
        if start + 64 > len(data):
            break
        payouts.append(int(data[start : start + 64], 16))
    # Normalize to 1 (Up) or 0 (Down) for binary markets
    if len(payouts) == 2:
        if payouts[0] == 1 and payouts[1] == 0:
            settled = 1
        elif payouts[0] == 0 and payouts[1] == 1:
            settled = 0
        else:
            settled = -1  # tie or invalid
    else:
        settled = -1
    return {
        "block_number": block_num,
        "timestamp": block_ts,
        "tx_hash": log.get("transactionHash"),
        "log_index": log.get("logIndex"),
        "question_id": topics[3],
        "settled_price": settled,
    }


# ── Log fetching ──────────────────────────────────────────────────────────────


def _fetch_sqd_raw(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> list[tuple[dict, int, int]]:
    """Fetch QuestionResolved logs from SQD Portal with stream continuation.

    SQD Portal may end the stream before reaching toBlock — the client must
    re-request from last_received_block + 1 until the full range is covered.
    Returns list of (log, block_num, block_ts).
    """
    results: list[tuple[dict, int, int]] = []
    current = from_block
    retries = 0

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
                    "address": [CTF_ADDRESS],
                    "topic0": [CONDITION_RESOLUTION_TOPIC],
                }
            ],
        }

        resp = None
        try:
            print(
                f"  Requesting blocks {current}-{to_block} ({to_block - current + 1} blocks)..."
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
                    continue
                if "header" not in obj:
                    continue
                block_num = obj["header"]["number"]
                block_ts = obj["header"]["timestamp"]
                current = block_num + 1
                for entry in obj.get("logs", []):
                    results.append((entry, block_num, block_ts))
            resp.close()
            if current > to_block:
                break
            # Stream ended early — SQD pagination, continue from current
            time.sleep(SQD_DELAY)
        except Exception as exc:
            if resp is not None:
                try:
                    resp.close()
                except Exception:
                    pass
            retries += 1
            if retries > MAX_RETRIES:
                raise
            delay = RETRY_BASE_DELAY * (2 ** (retries - 1))
            print(f"  Retry {retries}/{MAX_RETRIES} after {delay}s: {exc}")
            time.sleep(delay)

    return results


# ── Main fetch ────────────────────────────────────────────────────────────────


def fetch_events(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    day_start_ts: int,
    day_end_ts: int,
) -> list[dict]:
    """Fetch QuestionResolved events within [day_start_ts, day_end_ts).

    Returns one row per resolved market.
    """
    resolved: list[dict] = []

    current = start_block
    while current <= end_block:
        to_block = min(current + SQD_MAX_BLOCKS_PER_REQUEST - 1, end_block)
        logs = _fetch_sqd_raw(client, current, to_block)

        page_count = 0
        for log, block_num, block_ts in logs:
            if block_ts < day_start_ts or block_ts >= day_end_ts:
                continue
            res = _decode_resolution(log, block_num, block_ts)
            if res:
                resolved.append(res)
                page_count += 1

        print(f"  [{current}-{to_block}] resolved: {page_count}")
        current = to_block + 1
        time.sleep(SQD_DELAY)

    print(f"  Total: {len(resolved)} resolved")
    return resolved
