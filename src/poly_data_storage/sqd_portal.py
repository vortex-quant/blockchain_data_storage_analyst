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
    SQD_URL,
    WRITE_BATCH_SIZE,
)


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


def fetch_sqd_page(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> tuple[list[tuple[dict, int, int]], int]:
    """Fetch one page of logs from SQD Portal.

    Streams the HTTP response line-by-line to avoid loading the entire
    body into memory. Returns (logs, parse_failures) where logs is a list
    of (log, block_num, block_ts) tuples.
    """
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

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.post(SQD_URL, json=payload, timeout=120.0, stream=True)
            resp.raise_for_status()
            break
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                print(f"      SQD retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
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
            for log in obj.get("logs", []):
                results.append((log, block_num, block_ts))
    finally:
        resp.close()

    return results, parse_failures


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
            print(f"  [page {page}] WARNING: {parse_failures} unparseable lines")

        if not raw_logs:
            print(f"  [page {page}] blocks {current}-{to_block}: 0 logs")
            current = to_block + 1
            time.sleep(SQD_DELAY)
            continue

        page_decoded = 0
        for log, block_num, block_ts in raw_logs:
            if block_ts < day_start_ts or block_ts >= day_end_ts:
                continue

            trade = decode_v2_order_filled(log, block_num, block_ts)
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

        last_block = raw_logs[-1][1]
        print(
            f"  [page {page}] blocks {current}-{to_block}: {page_decoded} fills "
            f"(total: {total_decoded:,})"
        )
        del raw_logs
        current = last_block + 1
        time.sleep(SQD_DELAY)

    if batch:
        yield batch
