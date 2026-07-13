"""SQD Portal log fetching and V2 OrderFilled event decoding."""

from __future__ import annotations

import time
from collections.abc import Generator
from datetime import UTC, datetime

import niquests
import orjson

from poly_data_storage.constants import (
    EXCHANGE_V2,
    EXCHANGE_V2_LOWER,
    MAX_RETRIES,
    ORDER_FILLED_TOPIC,
    RETRY_BASE_DELAY,
    SQD_DELAY,
    SQD_MAX_BLOCKS_PER_REQUEST,
    SQD_MIN_BLOCKS_PER_REQUEST,
    SQD_URL,
    WRITE_BATCH_SIZE,
)
from poly_data_storage.logger import get_logger

log = get_logger()


def decode_v2_order_filled(log: dict, block_num: int, block_ts: int) -> dict | None:
    """Decode a V2 OrderFilled event from SQD Portal log format.

    V2 OrderFilled event layout:
        Topics: [event_sig, orderHash, maker, taker]
        Data:   [side(0=BUY/1=SELL), tokenId, makerAmountFilled,
                 takerAmountFilled, fee, builder, metadata]

    For BUY (side=0): makerAmount=USDC(6dec), takerAmount=shares(6dec)
    For SELL (side=1): makerAmount=shares(6dec), takerAmount=USDC(6dec)

    fill_role is "taker_aggregate" when taker == EXCHANGE_V2, else "maker".
    """
    topics = log.get("topics", [])
    if len(topics) < 4:
        return None

    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]
    fields = [int(data[i : i + 64], 16) for i in range(0, len(data), 64)]
    if len(fields) < 7:
        return None

    side_val = fields[0]
    token_id = fields[1]
    maker_amount = fields[2]
    taker_amount = fields[3]
    fee = fields[4]
    builder = "0x" + format(fields[5], "040x")[-40:]
    metadata = "0x" + format(fields[6], "064x")

    if side_val == 0:
        amount_usd = maker_amount / 1e6
        shares = taker_amount / 1e6
        price = maker_amount / taker_amount if taker_amount > 0 else 0.0
    elif side_val == 1:
        shares = maker_amount / 1e6
        amount_usd = taker_amount / 1e6
        price = taker_amount / maker_amount if maker_amount > 0 else 0.0
    else:
        return None

    taker_addr = "0x" + topics[3][26:].lower()
    fill_role = "taker_aggregate" if taker_addr == EXCHANGE_V2_LOWER else "maker"

    return {
        "block_number": block_num,
        "timestamp": block_ts,
        "block_time": datetime.fromtimestamp(block_ts, tz=UTC).isoformat(),
        "tx_hash": log.get("transactionHash"),
        "log_index": log.get("logIndex"),
        "order_hash": topics[1],
        "maker": "0x" + topics[2][26:],
        "taker": "0x" + topics[3][26:],
        "fill_role": fill_role,
        "side": "BUY" if side_val == 0 else "SELL",
        "token_id": str(token_id),
        "maker_amount_raw": str(maker_amount),
        "taker_amount_raw": str(taker_amount),
        "fee": str(fee),
        "builder": builder,
        "metadata": metadata,
        "amount_usd": amount_usd,
        "shares": shares,
        "price": price,
    }


def _fetch_sqd_raw(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> tuple[list[tuple[dict, int, int]], int]:
    """Single HTTP request to SQD Portal. Returns (logs, parse_failures)."""
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
                "address": [EXCHANGE_V2],
                "topic0": [ORDER_FILLED_TOPIC],
            }
        ],
    }

    block_count = to_block - from_block + 1
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            log.info(f"  Requesting blocks {from_block}-{to_block} ({block_count} blocks)...")
            resp = client.post(SQD_URL, json=payload, timeout=120.0, stream=True)
            resp.raise_for_status()
            break
        except Exception as exc:
            if attempt < max_attempts - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                log.info(f"      SQD retry {attempt + 1}/{max_attempts} after {delay}s: {exc}")
                time.sleep(delay)
            else:
                raise

    results: list[tuple[dict, int, int]] = []
    parse_failures = 0

    try:
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
            for entry in obj.get("logs", []):
                results.append((entry, block_num, block_ts))
    finally:
        resp.close()

    return results, parse_failures


def fetch_sqd_page(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> tuple[list[tuple[dict, int, int]], int]:
    """Fetch logs from SQD Portal, splitting block range on stream resets.

    If the response stream is reset (common on high-volume days where
    10k blocks produce too much data), the range is split in half and
    each half is fetched recursively until it succeeds or hits the
    minimum block window.
    """
    try:
        return _fetch_sqd_raw(client, from_block, to_block)
    except Exception as exc:
        block_count = to_block - from_block + 1
        if block_count <= SQD_MIN_BLOCKS_PER_REQUEST:
            raise
        log.info(f"      SQD stream reset for {from_block}-{to_block} ({block_count} blocks), splitting...")
        mid = from_block + block_count // 2 - 1
        left_logs, left_fails = fetch_sqd_page(client, from_block, mid)
        time.sleep(SQD_DELAY)
        right_logs, right_fails = fetch_sqd_page(client, mid + 1, to_block)
        return left_logs + right_logs, left_fails + right_fails


def stream_decoded_logs(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    day_start_ts: int,
    day_end_ts: int,
    batch_size: int = WRITE_BATCH_SIZE,
) -> Generator[list[dict], None, None]:
    """Fetch and decode OrderFilled logs, yielding batches of filtered rows.

    - Pages through SQD Portal block-by-block.
    - Filters by timestamp [day_start_ts, day_end_ts) to enforce exact UTC day.
    - Deduplicates by (tx_hash, log_index).
    - Yields batches of up to batch_size decoded rows for incremental writing.
    """
    seen: set[tuple[str, int]] = set()
    batch: list[dict] = []
    current = start_block
    page = 0
    total_decoded = 0

    while current <= end_block:
        to_block = min(current + SQD_MAX_BLOCKS_PER_REQUEST - 1, end_block)
        raw_logs, parse_failures = fetch_sqd_page(client, current, to_block)

        page += 1
        if parse_failures:
            log.info(f"  [page {page}] WARNING: {parse_failures} unparseable lines")

        if not raw_logs:
            log.info(f"  [page {page}] blocks {current}-{to_block}: 0 logs")
            current = to_block + 1
            time.sleep(SQD_DELAY)
            continue

        # Get exact time range from actual block timestamps in the data
        actual_min_ts = min(ts for _, _, ts in raw_logs)
        actual_max_ts = max(ts for _, _, ts in raw_logs)
        from_str = datetime.fromtimestamp(actual_min_ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        to_str = datetime.fromtimestamp(actual_max_ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")

        page_decoded = 0
        for raw_log, block_num, block_ts in raw_logs:
            if block_ts < day_start_ts or block_ts >= day_end_ts:
                continue

            trade = decode_v2_order_filled(raw_log, block_num, block_ts)
            if trade is None:
                continue

            key = (trade["tx_hash"], trade["log_index"])
            if key in seen:
                continue
            seen.add(key)

            batch.append(trade)
            page_decoded += 1
            total_decoded += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

        log.info(
            f"  [page {page}] blocks {current}-{to_block}: {page_decoded} fills "
            f"({from_str} to {to_str}, total: {total_decoded:,})"
        )
        del raw_logs
        current = to_block + 1
        time.sleep(SQD_DELAY)

    if batch:
        yield batch
