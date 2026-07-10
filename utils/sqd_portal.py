"""Polymarket SQD Portal data fetcher — SQD Portal log fetching and V2 event decoding."""

from __future__ import annotations

import gc
import time
from datetime import UTC, datetime

import niquests
import orjson

from utils.constants import (
    EXCHANGE_V2,
    MAX_RETRIES,
    ORDER_FILLED_TOPIC,
    RETRY_BASE_DELAY,
    SQD_DELAY,
    SQD_URL,
)


def decode_v2_order_filled(log: dict, block_num: int, block_ts: int) -> dict | None:
    """Decode a V2 OrderFilled event from SQD Portal log format.

    V2 OrderFilled event layout:
        Topics: [event_sig, orderHash, maker, taker]
        Data:   [side(0=BUY/1=SELL), tokenId, makerAmountFilled,
                 takerAmountFilled, fee, ...]

    For BUY (side=0): makerAmount=USDC(6dec), takerAmount=shares(6dec)
    For SELL (side=1): makerAmount=shares(6dec), takerAmount=USDC(6dec)
    """
    topics = log.get("topics", [])
    if len(topics) < 4:
        return None

    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]
    fields = [int(data[i : i + 64], 16) for i in range(0, len(data), 64)]
    if len(fields) < 5:
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

    return {
        "block_number": block_num,
        "block_time": datetime.fromtimestamp(block_ts, tz=UTC).isoformat(),
        "timestamp": block_ts,
        "tx_hash": log.get("transactionHash"),
        "log_index": log.get("logIndex"),
        "order_hash": topics[1],
        "maker": "0x" + topics[2][26:],
        "taker": "0x" + topics[3][26:],
        "side": "BUY" if side_val == 0 else "SELL",
        "token_id": str(token_id),
        "amount_usd": amount_usd,
        "shares": shares,
        "price": price,
        "maker_amount_raw": maker_amount,
        "taker_amount_raw": taker_amount,
        "fee": fee,
    }


def fetch_sqd_page(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> list[tuple[dict, int, int]]:
    """Fetch one page of logs from SQD Portal.

    Returns list of (log, block_num, block_ts).
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
            resp = client.post(SQD_URL, json=payload, timeout=120.0)
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
    for line in resp.content.strip().split(b"\n"):
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
        for log in obj.get("logs", []):
            results.append((log, block_num, block_ts))
    return results


def stream_decoded_logs(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    token_to_event: dict[int, int],
    token_to_outcome: dict[int, str],
    max_per_request: int = 10000,
) -> tuple[list[dict], int]:
    """Fetch and decode logs page-by-page to minimize memory usage.

    Instead of storing all raw logs + all decoded trades in memory,
    this decodes each page immediately and only keeps the decoded trades.
    Raw page data is freed after each page is processed.

    Returns (decoded_trades, total_pages).
    """
    token_id_set = set(token_to_event.keys())
    decoded: list[dict] = []
    current = start_block
    page = 0

    while current <= end_block:
        to_block = min(current + max_per_request - 1, end_block)
        raw_logs = fetch_sqd_page(client, current, to_block)

        page += 1
        if not raw_logs:
            print(f"  [page {page}] blocks {current}-{to_block}: 0 logs (empty)")
            current = to_block + 1
            continue

        page_decoded = 0
        for log, block_num, block_ts in raw_logs:
            trade = decode_v2_order_filled(log, block_num, block_ts)
            if trade is None:
                continue

            token_int = int(trade["token_id"])
            if token_int in token_id_set:
                trade["_event_id"] = token_to_event[token_int]
                trade["outcome"] = token_to_outcome.get(token_int, "")
            else:
                trade["_event_id"] = None
                trade["outcome"] = None

            decoded.append(trade)
            page_decoded += 1

        last_block = raw_logs[-1][1]
        print(
            f"  [page {page}] blocks {current}-{to_block}: {page_decoded} trades "
            f"(last block: {last_block}, total: {len(decoded):,})"
        )
        del raw_logs
        if page % 10 == 0:
            gc.collect()
        current = last_block + 1
        time.sleep(SQD_DELAY)

    return decoded, page
