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
    ORDER_FILLED_TOPIC,
    RETRY_BASE_DELAY,
    SQD_DELAY,
    SQD_MAX_BLOCKS_PER_REQUEST,
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

    is_taker_aggregate is True when taker == EXCHANGE_V2, else False (maker).
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
    is_taker_aggregate = taker_addr == EXCHANGE_V2_LOWER

    return {
        "block_number": block_num,
        "timestamp": block_ts,
        "block_time": datetime.fromtimestamp(block_ts, tz=UTC).isoformat(),
        "tx_hash": log.get("transactionHash"),
        "log_index": log.get("logIndex"),
        "order_hash": topics[1],
        "maker": "0x" + topics[2][26:],
        "taker": "0x" + topics[3][26:],
        "is_taker": is_taker_aggregate,
        "is_sell": side_val == 1,
        "token_id": str(token_id),
        "fee": fee / 1e6,
        "amount_usd": amount_usd,
        "shares": shares,
        "price": price,
    }


def _fetch_sqd_raw(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> Generator[tuple[dict, int, int], None, None]:
    """Stream logs from SQD Portal, yielding (log_dict, block_num, block_ts).

    Handles stream resets by continuing from the last yielded block.
    If the HTTP connection drops mid-stream, the next request starts
    at last_block + 1 — no data is skipped and no data is duplicated.
    Retries up to 5 times with exponential backoff before raising.
    """
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
                    "address": [EXCHANGE_V2],
                    "topic0": [ORDER_FILLED_TOPIC],
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
                    yield (entry, block_num, block_ts)
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
    batch_size: int = WRITE_BATCH_SIZE,
) -> Generator[list[dict], None, None]:
    """Fetch and decode OrderFilled logs, yielding batches of filtered rows.

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

        for raw_log, block_num, block_ts in _fetch_sqd_raw(client, current, to_block):
            last_block = block_num
            if page_min_ts is None or block_ts < page_min_ts:
                page_min_ts = block_ts
            if page_max_ts is None or block_ts > page_max_ts:
                page_max_ts = block_ts

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

        if page_min_ts is not None:
            from_str = datetime.fromtimestamp(page_min_ts, tz=UTC).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            to_str = datetime.fromtimestamp(page_max_ts, tz=UTC).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            log.info(
                f"  [page {page}] blocks {current}-{to_block}: {page_decoded} fills "
                f"({from_str} to {to_str}, total: {total_decoded:,})"
            )
        else:
            log.info(f"  [page {page}] blocks {current}-{to_block}: 0 logs")

        seen.clear()
        current = last_block + 1 if last_block >= current else to_block + 1
        time.sleep(SQD_DELAY)

    if batch:
        yield batch
