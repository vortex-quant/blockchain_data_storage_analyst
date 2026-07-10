"""Fetch all BTC up/down 5-minute events and trades for a specific day.

The Polymarket BTC 5-min events use a slug pattern:
    btc-updown-5m-{unix_timestamp}
where unix_timestamp is the event start time in seconds (UTC).

For a given day, there are 288 five-minute windows (24h × 12).
We compute all 288 timestamps, query Gamma API by slug in batches,
then fetch ALL trades from Polygon on-chain data.

Trade data sources (two strategies, both used):

1. ON-CHAIN (primary — complete, no cap):
   - Query CTF Exchange V1 contract's OrderFilled events via Polygon RPC
   - Fetches every individual order fill with maker/taker addresses, amounts
   - No 3500-trade cap — gets truly ALL trades
   - We scan the block range for the target day in chunks, filter by token IDs

2. DATA API (enrichment — adds metadata):
   - Provides outcome (Up/Down), side (BUY/SELL), price, size, user profiles
n   - Capped at 3500 trades per query but used to enrich on-chain data
   - We query with takerOnly=true + takerOnly=false and deduplicate

Final output merges on-chain completeness with Data API metadata.

Usage:
    uv run python fetch_btc_5min_day.py [YYYY-MM-DD] [--limit N] [--onchain-only]

Arguments:
    YYYY-MM-DD      Target date (default: 2026-06-21)
    --limit N       Only fetch first N events (for testing)
    --onchain-only  Skip Data API enrichment, only fetch on-chain trades

Outputs:
    btc_5min_events_YYYY-MM-DD.parquet — event metadata from Gamma API
    btc_5min_trades_YYYY-MM-DD.parquet — all trades (on-chain + enriched)
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

import httpx
import polars as pl

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"

# Polygon RPC — free tier, supports eth_getLogs with ~100 block ranges
POLYGON_RPC = "https://polygon.drpc.org"

# CTF Exchange V1 contract on Polygon
EXCHANGE_V1 = "0xE111180000d2663C0091e4f400237545B87B996B"
# OrderFilled event topic (computed from event signature)
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)

SLUG_PREFIX = "btc-updown-5m-"
SLUG_BATCH_SIZE = 20  # Gamma API caps at 20 slugs per request
DATA_PAGE_SIZE = 500
REQUEST_TIMEOUT = 30.0
RPC_TIMEOUT = 60.0
GAMMA_DELAY = 0.3
DATA_DELAY = 0.15
RPC_DELAY = 0.1
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
SAVE_EVERY_N_EVENTS = 50

# On-chain scanning parameters
BLOCK_RANGE_SIZE = 50  # blocks per eth_getLogs call (stay under response size limit)
BLOCK_BATCH_SIZE = 50  # blocks per batch getBlockByNumber call
POLYGON_BLOCK_TIME = 1.5  # seconds per block (Polygon produces ~2 blocks per 3 seconds)

ET_OFFSET_SECONDS = 4 * 3600  # EDT = UTC-4


def compute_5min_timestamps(date_str: str, tz_offset_seconds: int = 0) -> list[int]:
    """Compute all 288 Unix timestamps for 5-min windows on a given day.

    Args:
        date_str: Date in YYYY-MM-DD format.
        tz_offset_seconds: Timezone offset from UTC in seconds.
            0 = UTC, 4*3600 = EDT (Eastern Daylight Time).
    """
    target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_start_unix = int(target.timestamp()) + tz_offset_seconds
    return [day_start_unix + i * 300 for i in range(288)]


def _request_with_retry(
    client: httpx.Client,
    url: str,
    params: dict,
    expected_400: bool = False,
) -> httpx.Response | None:
    """HTTP GET with retry on transient errors.

    Args:
        expected_400: If True, a 400 response is expected (offset cap) and
            returns None instead of raising.

    Returns the Response on success, or None if expected_400 and 400 received.
    Raises HTTPStatusError on non-transient errors after exhausting retries.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(url, params=params)
            if resp.status_code == 400 and expected_400:
                return None
            if resp.status_code >= 500:
                raise httpx.HTTPStatusError(
                    f"Server error {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
            resp.raise_for_status()
            return resp
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                print(f"      Retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                time.sleep(delay)
            else:
                raise
    return None  # unreachable


def _rpc_post_with_retry(
    client: httpx.Client,
    payload: dict | list,
) -> dict | list:
    """JSON-RPC POST with retry on transient errors."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.post(POLYGON_RPC, json=payload, timeout=RPC_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                print(f"      RPC retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                time.sleep(delay)
            else:
                raise
    return {}  # unreachable


def extract_token_ids_from_events(events: list[dict]) -> dict[int, str]:
    """Extract all clobTokenIds from event market metadata.

    Returns a mapping: token_id_int -> event_id (for filtering on-chain events).
    """
    token_to_event: dict[int, str] = {}
    for event in events:
        markets = event.get("markets", [])
        if isinstance(markets, str):
            markets = json.loads(markets)
        for market in markets:
            tokens = market.get("clobTokenIds", [])
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            if isinstance(tokens, list):
                for token in tokens:
                    token_to_event[int(token)] = int(event["id"])
    return token_to_event


def extract_token_outcome_map(events: list[dict]) -> dict[int, str]:
    """Map token_id_int -> outcome name (e.g. 'Up' or 'Down')."""
    token_to_outcome: dict[int, str] = {}
    for event in events:
        markets = event.get("markets", [])
        if isinstance(markets, str):
            markets = json.loads(markets)
        for market in markets:
            tokens = market.get("clobTokenIds", [])
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(tokens, list) and isinstance(outcomes, list):
                for token, outcome in zip(tokens, outcomes):
                    token_to_outcome[int(token)] = outcome
    return token_to_outcome


def decode_order_filled(log: dict) -> list[dict]:
    """Decode a V1 OrderFilled event log into maker and taker trade records.

    Event signature:
        OrderFilled(bytes32 orderHash, address maker, address taker,
                    uint256 makerAssetId, uint256 takerAssetId,
                    uint256 makerAmountFilled, uint256 takerAmountFilled,
                    uint256 fee, ...)

    Topics: [event_sig, orderHash, maker, taker]
    Data:   [makerAssetId, takerAssetId, makerAmountFilled,
             takerAmountFilled, fee, ...]

    In the V1 CTF Exchange, makerAssetId is always 0 or 1 (USDC collateral)
    and takerAssetId is always the outcome token ID. This means:
    - Maker gives USDC, receives tokens -> maker is BUYING
    - Taker gives tokens, receives USDC -> taker is SELLING

    Returns a list of two trade dicts: [maker_trade, taker_trade].
    """
    topics = log.get("topics", [])
    if len(topics) < 4:
        return []

    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]
    fields = [int(data[i : i + 64], 16) for i in range(0, len(data), 64)]
    if len(fields) < 5:
        return []

    maker_asset_id = fields[0]
    taker_asset_id = fields[1]
    maker_amount = fields[2]
    taker_amount = fields[3]
    fee = fields[4]

    # Identify the outcome token (large number = token ID)
    if taker_asset_id > 100:
        token_id = taker_asset_id
        price = maker_amount / taker_amount if taker_amount > 0 else 0.0
        size = taker_amount / 1e6
    elif maker_asset_id > 100:
        token_id = maker_asset_id
        price = taker_amount / maker_amount if maker_amount > 0 else 0.0
        size = maker_amount / 1e6
    else:
        return []

    block_number = int(log.get("blockNumber", "0x0"), 16)
    log_index = int(log.get("logIndex", "0x0"), 16)
    tx_hash = log.get("transactionHash")
    order_hash = topics[1]
    maker_addr = "0x" + topics[2][26:]
    taker_addr = "0x" + topics[3][26:]

    # Maker trade: gives USDC, receives tokens -> BUY
    maker_trade = {
        "transactionHash": tx_hash,
        "blockNumber": block_number,
        "logIndex": log_index,
        "orderHash": order_hash,
        "maker": maker_addr,
        "taker": taker_addr,
        "tokenId": str(token_id),
        "side": "BUY",
        "role": "maker",
        "trader": maker_addr,
        "price": price,
        "size": size,
        "makerAmountFilled": maker_amount,
        "takerAmountFilled": taker_amount,
        "fee": fee,
    }

    # Taker trade: gives tokens, receives USDC -> SELL
    taker_trade = {
        "transactionHash": tx_hash,
        "blockNumber": block_number,
        "logIndex": log_index,
        "orderHash": order_hash,
        "maker": maker_addr,
        "taker": taker_addr,
        "tokenId": str(token_id),
        "side": "SELL",
        "role": "taker",
        "trader": taker_addr,
        "price": price,
        "size": size,
        "makerAmountFilled": maker_amount,
        "takerAmountFilled": taker_amount,
        "fee": fee,
    }

    return [maker_trade, taker_trade]


def find_block_for_timestamp(
    target_ts: int,
    known_block: int,
    known_block_ts: int,
) -> int:
    """Estimate the block number for a target Unix timestamp.

    Uses a known (block, timestamp) reference point and Polygon's ~1.5s block time.
    """
    delta_seconds = target_ts - known_block_ts
    delta_blocks = int(delta_seconds / POLYGON_BLOCK_TIME)
    return known_block + delta_blocks


def _parse_iso_to_unix(ts: str) -> int:
    """Parse an ISO 8601 timestamp string to Unix seconds."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp())


def fetch_onchain_trades_for_day(
    client: httpx.Client,
    target_date: str,
    token_to_event: dict[int, str],
    events: list[dict] | None = None,
) -> list[dict]:
    """Fetch ALL OrderFilled events from Polygon for a specific time range.

    Scans the CTF Exchange V1 contract's OrderFilled events in block-range
    chunks, filters for our token IDs, and decodes trade data.

    If `events` is provided, the block range is derived from the earliest and
    latest event start/end times (much faster for --limit testing). Otherwise
    the full day is scanned.
    """
    if events:
        # Use actual event time range instead of full day
        start_times = []
        end_times = []
        for ev in events:
            st = ev.get("startTime") or ev.get("startDate")
            et = ev.get("endDate") or ev.get("closedTime")
            if st:
                start_times.append(_parse_iso_to_unix(st))
            if et:
                end_times.append(_parse_iso_to_unix(et))
        scan_ts_start = min(start_times) if start_times else int(
            datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        )
        scan_ts_end = max(end_times) if end_times else scan_ts_start + 86400
        # Add 10-min buffer on each side
        scan_ts_start -= 600
        scan_ts_end += 600
    else:
        day_start = int(
            datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        )
        scan_ts_start = day_start
        scan_ts_end = day_start + 86400

    # Find a reference block by getting the latest block
    latest_resp = _rpc_post_with_retry(
        client,
        {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
    )
    latest_block = int(latest_resp["result"], 16)

    latest_block_data = _rpc_post_with_retry(
        client,
        {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(latest_block), False],
            "id": 1,
        },
    )
    latest_block_ts = int(latest_block_data["result"]["timestamp"], 16)

    # Estimate block range for the scan window
    est_start = find_block_for_timestamp(scan_ts_start, latest_block, latest_block_ts)
    est_end = find_block_for_timestamp(scan_ts_end, latest_block, latest_block_ts)

    # Calibrate: fetch the estimated start block and check its actual timestamp
    # This corrects for any drift in block time estimation
    cal_resp = _rpc_post_with_retry(
        client,
        {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(est_start), False],
            "id": 1,
        },
    )
    cal_block_ts = latest_block_ts  # fallback
    if cal_resp and isinstance(cal_resp, dict) and cal_resp.get("result"):
        actual_start_ts = int(cal_resp["result"]["timestamp"], 16)
        cal_block_ts = actual_start_ts
        ts_drift = actual_start_ts - scan_ts_start
        if abs(ts_drift) > 60:  # more than 1 minute off
            block_drift = int(ts_drift / POLYGON_BLOCK_TIME)
            est_start -= block_drift
            est_end -= block_drift
            # Re-calibrate after adjustment
            cal_resp2 = _rpc_post_with_retry(
                client,
                {
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [hex(est_start), False],
                    "id": 1,
                },
            )
            if cal_resp2 and isinstance(cal_resp2, dict) and cal_resp2.get("result"):
                cal_block_ts = int(cal_resp2["result"]["timestamp"], 16)
            print(f"  Calibrated: drift={ts_drift}s, adjusted by {block_drift} blocks")

    cal_block_num = est_start

    # Add buffer blocks on each side (block time can vary)
    buffer_blocks = 100
    scan_start = max(0, est_start - buffer_blocks)
    scan_end = est_end + buffer_blocks

    total_blocks = scan_end - scan_start
    n_calls = (total_blocks + BLOCK_RANGE_SIZE - 1) // BLOCK_RANGE_SIZE

    print(f"  Block range: {scan_start} to {scan_end} ({total_blocks} blocks, {n_calls} calls)")

    all_matching_logs: list[dict] = []
    token_id_set = set(token_to_event.keys())

    for i in range(0, total_blocks, BLOCK_RANGE_SIZE):
        from_block = scan_start + i
        to_block = min(from_block + BLOCK_RANGE_SIZE - 1, scan_end)

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [
                {
                    "address": EXCHANGE_V1,
                    "topics": [ORDER_FILLED_TOPIC],
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                }
            ],
            "id": 1,
        }

        try:
            response = _rpc_post_with_retry(client, payload)
        except Exception as exc:
            print(f"    Blocks {from_block}-{to_block}: FAILED - {exc}")
            continue

        if isinstance(response, dict) and "error" in response:
            # Response too large — try smaller range
            if from_block < to_block:
                mid = (from_block + to_block) // 2
                for fb, tb in [(from_block, mid), (mid + 1, to_block)]:
                    payload["params"][0]["fromBlock"] = hex(fb)
                    payload["params"][0]["toBlock"] = hex(tb)
                    payload["id"] = payload["params"][0]["fromBlock"]
                    try:
                        sub_resp = _rpc_post_with_retry(client, payload)
                        if isinstance(sub_resp, dict) and "result" in sub_resp:
                            logs = sub_resp["result"]
                            for log in logs:
                                decoded_list = decode_order_filled(log)
                                for decoded in decoded_list:
                                    if int(decoded["tokenId"]) in token_id_set:
                                        all_matching_logs.append(decoded)
                    except Exception:
                        pass
                    time.sleep(RPC_DELAY)
            continue

        logs = response.get("result", [])

        # Filter for our token IDs and decode
        for log in logs:
            decoded_list = decode_order_filled(log)
            for decoded in decoded_list:
                if int(decoded["tokenId"]) in token_id_set:
                    all_matching_logs.append(decoded)

        call_num = i // BLOCK_RANGE_SIZE + 1
        if call_num % 50 == 0 or call_num == 1 or call_num == n_calls:
            print(
                f"    [{call_num}/{n_calls}] blocks {from_block}-{to_block}: "
                f"{len(logs)} total logs, {len(all_matching_logs)} matching"
            )

        time.sleep(RPC_DELAY)

    # Compute timestamps from block numbers using the calibration reference.
    # This avoids hundreds of RPC calls for block timestamps.
    # Polygon block time is very consistent (~1.5s), so interpolation is accurate
    # to within a few seconds.
    print(f"  Computing timestamps from block numbers (calibrated at block {cal_block_num})...")
    for log in all_matching_logs:
        block_delta = log["blockNumber"] - cal_block_num
        log["timestamp"] = int(cal_block_ts + block_delta * POLYGON_BLOCK_TIME)
        log["_event_id"] = token_to_event.get(int(log["tokenId"]))

    return all_matching_logs


def fetch_events_by_slugs(
    client: httpx.Client,
    slugs: list[str],
) -> list[dict]:
    """Fetch events from Gamma API by slug in batches."""
    all_events: list[dict] = []

    for i in range(0, len(slugs), SLUG_BATCH_SIZE):
        batch = slugs[i : i + SLUG_BATCH_SIZE]
        resp = _request_with_retry(
            client,
            f"{GAMMA_BASE}/events",
            params={"slug": batch, "closed": "true"},
        )
        if resp is None:
            continue
        results = resp.json()
        if isinstance(results, list):
            all_events.extend(results)

        n_batches = (len(slugs) + SLUG_BATCH_SIZE - 1) // SLUG_BATCH_SIZE
        print(
            f"    Slug batch {i // SLUG_BATCH_SIZE + 1}/{n_batches}: "
            f"got {len(results) if isinstance(results, list) else 0} events "
            f"(running total: {len(all_events)})"
        )
        time.sleep(GAMMA_DELAY)

    return all_events


def run_for_timezone(
    client: httpx.Client,
    date_str: str,
    tz_name: str,
    tz_offset: int,
) -> tuple[list[dict], list[str]]:
    """Try fetching events for a specific timezone interpretation of the date."""
    print(f"\n  Trying {tz_name} interpretation...")

    timestamps = compute_5min_timestamps(date_str, tz_offset)
    slugs = [f"{SLUG_PREFIX}{ts}" for ts in timestamps]

    first_ts = datetime.fromtimestamp(timestamps[0], tz=UTC)
    last_ts = datetime.fromtimestamp(timestamps[-1], tz=UTC)
    print(f"    First window: {first_ts.isoformat()} → slug: {slugs[0]}")
    print(f"    Last window:  {last_ts.isoformat()} → slug: {slugs[-1]}")

    events = fetch_events_by_slugs(client, slugs)
    events.sort(key=lambda e: e.get("slug") or "")

    return events, slugs


def _fetch_trades_paginated(
    client: httpx.Client,
    event_id: int,
    taker_only: bool,
) -> tuple[list[dict], bool]:
    """Fetch trades from Data API with pagination.

    Returns (trades, hit_cap) where hit_cap indicates whether the 3500-trade
    limit was reached (meaning some trades may be missing).
    """
    all_trades: list[dict] = []
    offset = 0
    params = {
        "eventId": event_id,
        "limit": DATA_PAGE_SIZE,
        "takerOnly": str(taker_only).lower(),
    }

    while True:
        resp = _request_with_retry(
            client,
            f"{DATA_BASE}/trades",
            params={**params, "offset": offset},
            expected_400=True,
        )
        if resp is None:
            # 400 = offset cap exceeded (expected at offset >= 3500)
            break

        batch = resp.json()
        if not batch:
            break

        all_trades.extend(batch)

        if len(batch) < DATA_PAGE_SIZE:
            break

        offset += DATA_PAGE_SIZE
        time.sleep(DATA_DELAY)

    hit_cap = len(all_trades) >= 3500
    return all_trades, hit_cap


def fetch_all_trades_for_event(
    client: httpx.Client,
    event: dict,
) -> tuple[list[dict], bool]:
    """Fetch all trades for an event from the Data API.

    Strategy: fetch with both takerOnly=true and takerOnly=false,
    then deduplicate by (transactionHash, side, asset, price, size).

    Returns (trades, may_be_incomplete) where may_be_incomplete indicates
    whether the 3500-trade cap was hit on the takerOnly=false query.
    """
    event_id = int(event["id"])

    taker_trades, _ = _fetch_trades_paginated(client, event_id, taker_only=True)
    time.sleep(DATA_DELAY)
    all_trades_raw, hit_cap = _fetch_trades_paginated(client, event_id, taker_only=False)

    # Deduplicate by (transactionHash, side, asset, price, size)
    seen: set[tuple] = set()
    combined: list[dict] = []

    for trade in taker_trades + all_trades_raw:
        key = (
            trade.get("transactionHash"),
            trade.get("side"),
            trade.get("asset"),
            trade.get("price"),
            trade.get("size"),
        )
        if key not in seen:
            seen.add(key)
            combined.append(trade)

    return combined, hit_cap


def _save_trades(
    all_trades: list[dict],
    target_date: str,
) -> str:
    """Save trades to parquet and return the filename."""
    if not all_trades:
        return ""

    trades_df = pl.DataFrame(all_trades)

    if "timestamp" in trades_df.columns:
        trades_df = trades_df.with_columns(
            pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="s")
            .cast(pl.Datetime("us"))
            .alias("datetime")
        )

    trades_file = f"btc_5min_trades_{target_date}.parquet"
    trades_df.write_parquet(trades_file)
    return trades_file


def enrich_onchain_with_data_api(
    onchain_trades: list[dict],
    data_api_trades: list[dict],
    token_to_outcome: dict[int, str],
) -> list[dict]:
    """Merge on-chain trades with Data API metadata.

    On-chain trades have: maker, taker, tokenId, side, price, size, blockNumber
    Data API trades have: outcome, side, price, size, proxyWallet, pseudonym, etc.

    We match by transactionHash and add outcome + user metadata to on-chain trades.
    """
    # Build lookup: (transactionHash) -> list of Data API trades
    api_by_tx: dict[str, list[dict]] = {}
    for trade in data_api_trades:
        tx = trade.get("transactionHash")
        if tx:
            api_by_tx.setdefault(tx, []).append(trade)

    # Add outcome to on-chain trades from token mapping
    for trade in onchain_trades:
        token_id = trade.get("tokenId")
        if token_id:
            token_int = int(token_id) if isinstance(token_id, str) else token_id
            if token_int in token_to_outcome:
                trade["outcome"] = token_to_outcome[token_int]

        # Try to enrich with Data API metadata
        tx = trade.get("transactionHash")
        if tx and tx in api_by_tx:
            api_trades = api_by_tx[tx]
            # Find matching trade by side or closest price
            for api_trade in api_trades:
                trade["proxyWallet"] = api_trade.get("proxyWallet")
                trade["pseudonym"] = api_trade.get("pseudonym")
                trade["bio"] = api_trade.get("bio")
                trade["profileImage"] = api_trade.get("profileImage")
                break

    return onchain_trades


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch BTC up/down 5-min events and trades for a specific day."
    )
    parser.add_argument(
        "date",
        nargs="?",
        default="2026-06-21",
        help="Target date in YYYY-MM-DD format (default: 2026-06-21)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only fetch first N events (for testing)",
    )
    parser.add_argument(
        "--onchain-only",
        action="store_true",
        help="Skip Data API enrichment, only fetch on-chain trades",
    )
    args = parser.parse_args()
    target_date = args.date
    limit = args.limit
    onchain_only = args.onchain_only

    print(f"\n{'=' * 60}")
    print(f"  BTC Up/Down 5-Min Events — {target_date}")
    if limit:
        print(f"  [TEST MODE] Limiting to first {limit} events")
    if onchain_only:
        print(f"  [ON-CHAIN ONLY] Skipping Data API enrichment")
    print(f"{'=' * 60}")

    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
        # ── Step 1: Fetch events by slug ──────────────────────────────
        print(f"\n--- Step 1: Fetching events by slug ---")

        # Try UTC first
        events, expected_slugs = run_for_timezone(
            client, target_date, "UTC", tz_offset=0
        )
        print(f"\n    UTC result: {len(events)} events found")

        # If not 288, try EDT (Eastern Daylight Time, UTC-4)
        if len(events) != 288:
            print(f"\n    UTC gave {len(events)} events (expected 288). Trying EDT...")
            edt_events, edt_slugs = run_for_timezone(
                client, target_date, "EDT (UTC-4)", tz_offset=ET_OFFSET_SECONDS
            )
            print(f"\n    EDT result: {len(edt_events)} events found")

            if len(edt_events) > len(events):
                events = edt_events
                expected_slugs = edt_slugs
                print(f"    Using EDT interpretation ({len(events)} events)")
            else:
                print(f"    UTC interpretation gave more events, keeping it")

        # Verify
        print(f"\n  Total events found: {len(events)}")
        expected = 288
        if len(events) != expected:
            print(f"  ⚠ Expected {expected} events, got {len(events)}")

            found_slugs = {ev.get("slug") for ev in events}
            missing_slugs = [s for s in expected_slugs if s not in found_slugs]
            if missing_slugs:
                print(f"  Missing {len(missing_slugs)} slugs (first 5): {missing_slugs[:5]}")
        else:
            print(f"  ✓ Confirmed {expected} events")

        if not events:
            print("  No events found! Exiting.")
            return

        # Print samples
        print(f"\n  First 3 events:")
        for ev in events[:3]:
            print(
                f"    ID={ev.get('id')}, "
                f"Title='{ev.get('title')}', "
                f"slug={ev.get('slug')}, "
                f"startTime={ev.get('startTime')}"
            )

        print(f"  Last 3 events:")
        for ev in events[-3:]:
            print(
                f"    ID={ev.get('id')}, "
                f"Title='{ev.get('title')}', "
                f"slug={ev.get('slug')}, "
                f"startTime={ev.get('startTime')}"
            )

        # Save event metadata
        events_df = pl.DataFrame(events)
        events_file = f"btc_5min_events_{target_date}.parquet"
        events_df.write_parquet(events_file)
        print(f"\n  Saved {len(events)} events to {events_file}")
        print(f"  Events DataFrame: {events_df.shape}")
        print(f"  Columns: {events_df.columns}")

        # Extract token ID mappings for on-chain filtering
        events_to_fetch = events[:limit] if limit else events
        token_to_event = extract_token_ids_from_events(events_to_fetch)
        token_to_outcome = extract_token_outcome_map(events_to_fetch)
        print(f"\n  Extracted {len(token_to_event)} token IDs from {len(events_to_fetch)} events")

        # ── Step 2: Fetch ALL trades from on-chain ────────────────────
        print(f"\n--- Step 2: Fetching ALL trades from Polygon on-chain ---")
        print(f"  Scanning OrderFilled events on CTF Exchange V1")
        print(f"  This gets EVERY trade — no 3500 cap")

        onchain_trades = fetch_onchain_trades_for_day(
            client, target_date, token_to_event, events_to_fetch
        )

        print(f"\n  On-chain trades found: {len(onchain_trades)}")

        if not onchain_trades:
            print("  No on-chain trades found! Check block range estimation.")
            print("  Falling back to Data API only...")
            onchain_only = False

        # ── Step 3: Data API enrichment (optional) ────────────────────
        data_api_trades: list[dict] = []
        if not onchain_only and onchain_trades:
            print(f"\n--- Step 3: Data API enrichment ---")
            print(f"  Fetching metadata for {len(events_to_fetch)} events...")

            events_with_no_trades: list[int] = []
            events_at_cap: list[int] = []

            for i, ev in enumerate(events_to_fetch):
                event_id = int(ev["id"])
                trades, hit_cap = fetch_all_trades_for_event(client, ev)

                for t in trades:
                    t["_event_id"] = event_id

                data_api_trades.extend(trades)

                if not trades:
                    events_with_no_trades.append(event_id)
                if hit_cap:
                    events_at_cap.append(event_id)

                if (i + 1) % 50 == 0 or i == 0 or i == len(events_to_fetch) - 1:
                    cap_flag = " ⚠CAP" if hit_cap else ""
                    print(
                        f"    [{i + 1}/{len(events_to_fetch)}] "
                        f"event_id={event_id}: "
                        f"{len(trades)} trades{cap_flag}, "
                        f"running total={len(data_api_trades)}"
                    )

                if (i + 1) % SAVE_EVERY_N_EVENTS == 0:
                    print(f"    [checkpoint] {len(data_api_trades)} Data API trades so far")

                time.sleep(DATA_DELAY)

            print(f"\n  Data API trades: {len(data_api_trades)}")
            print(f"  Events at 3500 cap: {len(events_at_cap)}")
            if events_at_cap:
                print(f"    IDs (first 10): {events_at_cap[:10]}")

            # Merge on-chain with Data API metadata
            print(f"\n  Merging on-chain ({len(onchain_trades)}) with Data API ({len(data_api_trades)})...")
            all_trades = enrich_onchain_with_data_api(
                onchain_trades, data_api_trades, token_to_outcome
            )
        else:
            # On-chain only — add outcome from token mapping
            for trade in onchain_trades:
                token_id = trade.get("tokenId")
                if token_id:
                    token_int = int(token_id) if isinstance(token_id, str) else token_id
                    if token_int in token_to_outcome:
                        trade["outcome"] = token_to_outcome[token_int]
            all_trades = onchain_trades

        # ── Step 4: Save all trades ───────────────────────────────────
        print(f"\n--- Step 4: Saving trades ---")
        print(f"  Total trades: {len(all_trades)}")

        if not all_trades:
            print("  No trades found! Exiting.")
            return

        trades_file = _save_trades(all_trades, target_date)
        trades_df = pl.read_parquet(trades_file)
        print(f"  Saved to {trades_file}")
        print(f"  Trades DataFrame: {trades_df.shape}")
        print(f"  Columns: {trades_df.columns}")

        # ── Summary ───────────────────────────────────────────────────
        print(f"\n--- Summary ---")

        if "datetime" in trades_df.columns:
            print(f"  Time range: {trades_df['datetime'].min()} → {trades_df['datetime'].max()}")

        if "price" in trades_df.columns:
            print(f"  Price range: {trades_df['price'].min():.4f} → {trades_df['price'].max():.4f}")

        if "size" in trades_df.columns:
            print(f"  Total volume (shares): {trades_df['size'].sum():.2f}")

        if "side" in trades_df.columns:
            print(f"  Side counts:\n{trades_df['side'].value_counts()}")

        if "outcome" in trades_df.columns:
            print(f"  Outcome counts:\n{trades_df['outcome'].value_counts()}")

        if "_event_id" in trades_df.columns:
            per_event = trades_df.group_by("_event_id").len().sort("_event_id")
            print(
                f"  Trades per event: "
                f"min={per_event['len'].min()}, "
                f"max={per_event['len'].max()}, "
                f"median={per_event['len'].median()}"
            )

        print(f"\n  Data source: {'on-chain only' if onchain_only else 'on-chain + Data API enrichment'}")
        print(f"\n{'=' * 60}")
        print(f"  Done! Files saved:")
        print(f"    {events_file}")
        print(f"    {trades_file}")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
