"""SQD Portal — block resolution, log fetching, and UMA event decoding.

Fetches QuestionInitialized and QuestionResolved events from UMA CTF Adapter
contracts on Polygon, joins them on questionID, and parses ancillary data.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import niquests
import orjson

from poly_events_resolve_storage.constants import (
    ASSET_KEYWORDS,
    EVENT_TYPE_KEYWORDS,
    MAX_RETRIES,
    QUESTION_INITIALIZED_TOPIC,
    QUESTION_RESOLVED_TOPIC,
    RETRY_BASE_DELAY,
    SQD_DELAY,
    SQD_MAX_BLOCKS_PER_REQUEST,
    SQD_TIMESTAMP_URL,
    SQD_URL,
    UMA_CTF_ADAPTERS,
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


def _decode_initialized(log: dict) -> dict | None:
    """Decode QuestionInitialized(bytes ancillaryData, bytes32 indexed questionID)."""
    topics = log.get("topics", [])
    if len(topics) < 2:
        return None
    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 128:
        return None
    length = int(data[64:128], 16)
    content_hex = data[128 : 128 + length * 2]
    ancillary = bytes.fromhex(content_hex).decode("utf-8", errors="replace")
    return {"question_id": topics[1], "ancillary_data": ancillary}


def _decode_resolved(log: dict, block_num: int, block_ts: int) -> dict | None:
    """Decode QuestionResolved(bytes32 indexed questionID, int256 indexed settledPrice, uint256[] payouts)."""
    topics = log.get("topics", [])
    if len(topics) < 3:
        return None
    settled = int(topics[2], 16)
    if settled >= 2**255:
        settled -= 2**256
    return {
        "block_number": block_num,
        "timestamp": block_ts,
        "tx_hash": log.get("transactionHash"),
        "log_index": log.get("logIndex"),
        "question_id": topics[1],
        "settled_price": settled,
    }


def _parse_ancillary(text: str) -> tuple[str, str]:
    """Extract asset and event_type from ancillary data text."""
    lower = text.lower()
    asset = ""
    for keyword, symbol in ASSET_KEYWORDS:
        if keyword in lower:
            asset = symbol
            break
    event_type = ""
    for keyword, etype in EVENT_TYPE_KEYWORDS:
        if keyword in lower:
            event_type = etype
            break
    return asset, event_type


# ── Log fetching ──────────────────────────────────────────────────────────────


def _fetch_sqd_raw(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> list[tuple[dict, int, int]]:
    """Fetch logs from SQD Portal. Returns list of (log, block_num, block_ts)."""
    payload = {
        "type": "evm",
        "fromBlock": from_block,
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
                "address": UMA_CTF_ADAPTERS,
                "topic0": [QUESTION_INITIALIZED_TOPIC, QUESTION_RESOLVED_TOPIC],
            }
        ],
    }

    results: list[tuple[dict, int, int]] = []
    retries = 0

    while True:
        try:
            print(
                f"  Requesting blocks {from_block}-{to_block} ({to_block - from_block + 1} blocks)..."
            )
            resp = client.post(SQD_URL, json=payload, timeout=120.0, stream=True)
            resp.raise_for_status()
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
                for entry in obj.get("logs", []):
                    results.append((entry, block_num, block_ts))
            resp.close()
            break
        except Exception as exc:
            retries += 1
            if retries > MAX_RETRIES:
                raise
            delay = RETRY_BASE_DELAY * (2 ** (retries - 1))
            print(f"  Retry {retries}/{MAX_RETRIES} after {delay}s: {exc}")
            time.sleep(delay)

    return results


# ── Main fetch + join ─────────────────────────────────────────────────────────


def fetch_events(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    day_start_ts: int,
    day_end_ts: int,
) -> list[dict]:
    """Fetch UMA events, join Initialized+Resolved, parse ancillary data.

    Returns one row per resolved market within [day_start_ts, day_end_ts).
    """
    initialized: dict[str, dict] = {}
    resolved: list[dict] = []

    current = start_block
    while current <= end_block:
        to_block = min(current + SQD_MAX_BLOCKS_PER_REQUEST - 1, end_block)
        logs = _fetch_sqd_raw(client, current, to_block)

        page_init = page_resolved = 0
        for log, block_num, block_ts in logs:
            topic0 = log.get("topics", [""])[0]

            if topic0 == QUESTION_INITIALIZED_TOPIC:
                init = _decode_initialized(log)
                if init and init["question_id"] not in initialized:
                    initialized[init["question_id"]] = init
                    page_init += 1

            elif topic0 == QUESTION_RESOLVED_TOPIC:
                if block_ts < day_start_ts or block_ts >= day_end_ts:
                    continue
                res = _decode_resolved(log, block_num, block_ts)
                if res:
                    resolved.append(res)
                    page_resolved += 1

        print(f"  [{current}-{to_block}] init: {page_init}, resolved: {page_resolved}")
        current = to_block + 1
        time.sleep(SQD_DELAY)

    # Join resolved with initialized on question_id
    rows: list[dict] = []
    for res in resolved:
        init = initialized.get(res["question_id"])
        ancillary = init["ancillary_data"] if init else ""
        asset, event_type = _parse_ancillary(ancillary) if ancillary else ("", "")
        rows.append(
            {
                **res,
                "ancillary_data": ancillary,
                "asset": asset,
                "event_type": event_type,
            }
        )

    print(
        f"  Total: {len(initialized)} initialized, {len(resolved)} resolved, {len(rows)} joined rows"
    )
    return rows
