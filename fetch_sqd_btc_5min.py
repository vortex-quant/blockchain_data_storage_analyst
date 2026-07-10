"""Fetch all BTC up/down 5-minute events and trades for a specific day using SQD Portal.

SQD Portal is a free, no-API-key blockchain data service that streams EVM logs.
It has no block range limits and returns pre-indexed logs with block timestamps.

This script:
1. Computes all 288 5-min BTC event slugs for the target date
2. Fetches event metadata (token IDs, outcomes) from Gamma API
3. Fetches ALL OrderFilled V2 events from SQD Portal for the day's block range
4. Filters by BTC token IDs and decodes V2 event data
5. Saves to parquet files

Usage:
    uv run python fetch_sqd_btc_5min.py [YYYY-MM-DD] [--limit N]

Outputs:
    btc_5min_events_YYYY-MM-DD.parquet — event metadata from Gamma API
    btc_5min_trades_YYYY-MM-DD.parquet — all trades from SQD Portal
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

import niquests
import polars as pl

# ── Constants ──────────────────────────────────────────────────────────────────

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
GAMMA_BASE = "https://gamma-api.polymarket.com"

EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

SLUG_PREFIX = "btc-updown-5m-"
SLUG_BATCH_SIZE = 20
GAMMA_DELAY = 0.3
SQD_DELAY = 0.1
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0
POLYGON_BLOCK_TIME = 1.5  # seconds per block

ET_OFFSET_SECONDS = 4 * 3600  # EDT = UTC-4


# ── Event slug computation ────────────────────────────────────────────────────

def compute_5min_timestamps(date_str: str, tz_offset_seconds: int = 0) -> list[int]:
    """Compute all 288 Unix timestamps for 5-min windows on a given day."""
    target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_start_unix = int(target.timestamp()) + tz_offset_seconds
    return [day_start_unix + i * 300 for i in range(288)]


# ── Gamma API ─────────────────────────────────────────────────────────────────

def fetch_events_by_slugs(
    client: niquests.Session,
    slugs: list[str],
) -> list[dict]:
    """Fetch events from Gamma API by slug in batches."""
    all_events: list[dict] = []

    for i in range(0, len(slugs), SLUG_BATCH_SIZE):
        batch = slugs[i : i + SLUG_BATCH_SIZE]
        for attempt in range(MAX_RETRIES):
            try:
                resp = client.get(
                    f"{GAMMA_BASE}/events",
                    params={"slug": batch, "closed": "true"},
                    timeout=30.0,
                )
                resp.raise_for_status()
                results = resp.json()
                if isinstance(results, list):
                    all_events.extend(results)
                break
            except Exception as exc:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    print(f"      Retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                    time.sleep(delay)
                else:
                    print(f"      FAILED slug batch: {exc}")

        n_batches = (len(slugs) + SLUG_BATCH_SIZE - 1) // SLUG_BATCH_SIZE
        print(
            f"    Slug batch {i // SLUG_BATCH_SIZE + 1}/{n_batches}: "
            f"got {len(results) if isinstance(results, list) else 0} events "
            f"(running total: {len(all_events)})"
        )
        time.sleep(GAMMA_DELAY)

    return all_events


def extract_token_ids_from_events(events: list[dict]) -> dict[int, str]:
    """Extract all clobTokenIds from event market metadata.
    Returns mapping: token_id_int -> event_id.
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


# ── SQD Portal ────────────────────────────────────────────────────────────────

def decode_v2_order_filled(log: dict, block_num: int, block_ts: int) -> dict | None:
    """Decode a V2 OrderFilled event from SQD Portal log format.

    V2 OrderFilled event layout:
        Topics: [event_sig, orderHash, maker, taker]
        Data:   [side(0=BUY/1=SELL), tokenId, makerAmountFilled,
                 takerAmountFilled, fee, builder, metadata]

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
    """Fetch one page of logs from SQD Portal. Returns list of (log, block_num, block_ts)."""
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

    results = []
    for line in resp.text.strip().split("\n"):
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "header" not in obj:
            continue
        block_num = obj["header"]["number"]
        block_ts = obj["header"]["timestamp"]
        for log in obj.get("logs", []):
            results.append((log, block_num, block_ts))
    return results


def fetch_all_sqd_logs(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    max_per_request: int = 10000,
) -> list[tuple[dict, int, int]]:
    """Fetch all logs in a block range, handling SQD Portal pagination.

    SQD Portal returns a variable number of blocks per response (typically ~890-1000),
    limited by response size. We paginate by using the last returned block + 1.
    """
    all_logs: list[tuple[dict, int, int]] = []
    current = start_block
    page = 0

    while current <= end_block:
        to_block = min(current + max_per_request - 1, end_block)
        logs = fetch_sqd_page(client, current, to_block)

        page += 1
        if not logs:
            print(f"  [page {page}] blocks {current}-{to_block}: 0 logs (empty)")
            current = to_block + 1
            continue

        all_logs.extend(logs)
        last_block = logs[-1][1]
        print(
            f"  [page {page}] blocks {current}-{to_block}: {len(logs)} logs "
            f"(last block: {last_block}, total: {len(all_logs)})"
        )
        current = last_block + 1
        time.sleep(SQD_DELAY)

    return all_logs


# ── Block estimation ──────────────────────────────────────────────────────────

def find_block_for_timestamp(
    target_ts: int,
    known_block: int,
    known_block_ts: int,
) -> int:
    """Estimate block number for a target Unix timestamp using ~1.5s block time."""
    delta_seconds = target_ts - known_block_ts
    delta_blocks = int(delta_seconds / POLYGON_BLOCK_TIME)
    return known_block + delta_blocks


def get_latest_block_info(client: niquests.Session) -> tuple[int, int]:
    """Get latest block number and timestamp from SQD Portal."""
    logs = fetch_sqd_page(client, 0, 0)
    if logs:
        return logs[-1][1], logs[-1][2]
    # Fallback: use a recent block we know
    return 89885600, 1783533395


# ── Main ──────────────────────────────────────────────────────────────────────

def run_for_timezone(
    client: niquests.Session,
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
    print(f"    First window: {first_ts.isoformat()} -> slug: {slugs[0]}")
    print(f"    Last window:  {last_ts.isoformat()} -> slug: {slugs[-1]}")

    events = fetch_events_by_slugs(client, slugs)
    events.sort(key=lambda e: e.get("slug") or "")

    return events, slugs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch BTC up/down 5-min events and trades via SQD Portal (free, no API key)."
    )
    parser.add_argument(
        "date",
        nargs="?",
        default="2026-07-09",
        help="Target date in YYYY-MM-DD format (default: 2026-07-09)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only fetch first N events (for testing)",
    )
    args = parser.parse_args()
    target_date = args.date
    limit = args.limit

    print(f"\n{'=' * 60}")
    print(f"  BTC Up/Down 5-Min Events — {target_date} (SQD Portal)")
    print(f"{'=' * 60}")

    with niquests.Session() as client:
        # ── Step 1: Fetch events by slug from Gamma API ─────────────
        print(f"\n--- Step 1: Fetching events by slug ---")

        events, expected_slugs = run_for_timezone(
            client, target_date, "UTC", tz_offset=0
        )
        print(f"\n    UTC result: {len(events)} events found")

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

        print(f"\n  Total events found: {len(events)}")
        if len(events) != 288:
            print(f"  WARNING: Expected 288 events, got {len(events)}")
            found_slugs = {ev.get("slug") for ev in events}
            missing_slugs = [s for s in expected_slugs if s not in found_slugs]
            if missing_slugs:
                print(f"  Missing {len(missing_slugs)} slugs (first 5): {missing_slugs[:5]}")
        else:
            print(f"  OK: Confirmed 288 events")

        if not events:
            print("  No events found! Exiting.")
            return

        # Print samples
        print(f"\n  First 3 events:")
        for ev in events[:3]:
            print(
                f"    ID={ev.get('id')}, "
                f"Title='{ev.get('title')}', "
                f"slug={ev.get('slug')}"
            )
        print(f"  Last 3 events:")
        for ev in events[-3:]:
            print(
                f"    ID={ev.get('id')}, "
                f"Title='{ev.get('title')}', "
                f"slug={ev.get('slug')}"
            )

        # Save event metadata
        events_df = pl.DataFrame(events)
        events_file = f"btc_5min_events_{target_date}.parquet"
        events_df.write_parquet(events_file)
        print(f"\n  Saved {len(events)} events to {events_file}")

        # Extract token ID mappings
        events_to_fetch = events[:limit] if limit else events
        token_to_event = extract_token_ids_from_events(events_to_fetch)
        token_to_outcome = extract_token_outcome_map(events_to_fetch)
        print(f"  Extracted {len(token_to_event)} token IDs from {len(events_to_fetch)} events")

        # ── Step 2: Determine block range for the day ────────────────
        print(f"\n--- Step 2: Estimating block range ---")

        # Get latest block from SQD for calibration
        latest_block, latest_ts = get_latest_block_info(client)
        print(f"  Latest known block: {latest_block} (ts: {datetime.fromtimestamp(latest_ts, tz=UTC)})")

        # Compute scan window from event times + buffer
        start_times = []
        end_times = []
        for ev in events_to_fetch:
            st = ev.get("startTime") or ev.get("startDate")
            et = ev.get("endDate") or ev.get("closedTime")
            if st:
                ts = st.replace("Z", "+00:00") if isinstance(st, str) else st
                start_times.append(int(datetime.fromisoformat(ts).timestamp()))
            if et:
                ts = et.replace("Z", "+00:00") if isinstance(et, str) else et
                end_times.append(int(datetime.fromisoformat(ts).timestamp()))

        if start_times and end_times:
            scan_ts_start = min(start_times) - 600  # 10-min buffer
            scan_ts_end = max(end_times) + 600
        else:
            day_start = int(
                datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
            )
            scan_ts_start = day_start
            scan_ts_end = day_start + 86400

        est_start = find_block_for_timestamp(scan_ts_start, latest_block, latest_ts)
        est_end = find_block_for_timestamp(scan_ts_end, latest_block, latest_ts)

        # Add buffer blocks
        buffer = 200
        scan_start = max(0, est_start - buffer)
        scan_end = est_end + buffer
        total_blocks = scan_end - scan_start

        print(f"  Scan window: {datetime.fromtimestamp(scan_ts_start, tz=UTC)} -> {datetime.fromtimestamp(scan_ts_end, tz=UTC)}")
        print(f"  Block range: {scan_start} to {scan_end} ({total_blocks} blocks)")

        # ── Step 3: Fetch ALL OrderFilled logs from SQD Portal ──────
        print(f"\n--- Step 3: Fetching OrderFilled logs from SQD Portal ---")
        print(f"  This gets EVERY trade — no API key, no cap, free")

        t0 = time.time()
        raw_logs = fetch_all_sqd_logs(client, scan_start, scan_end)
        t1 = time.time()
        print(f"\n  Fetched {len(raw_logs)} total OrderFilled logs in {t1 - t0:.1f}s")

        # ── Step 4: Decode and filter by BTC token IDs ───────────────
        print(f"\n--- Step 4: Decoding and filtering by BTC token IDs ---")
        token_id_set = set(token_to_event.keys())

        btc_trades: list[dict] = []
        all_decoded: list[dict] = []

        for log, block_num, block_ts in raw_logs:
            decoded = decode_v2_order_filled(log, block_num, block_ts)
            if decoded is None:
                continue
            all_decoded.append(decoded)

            token_int = int(decoded["token_id"])
            if token_int in token_id_set:
                decoded["_event_id"] = token_to_event[token_int]
                decoded["outcome"] = token_to_outcome.get(token_int, "")
                btc_trades.append(decoded)

        print(f"  Total decoded trades: {len(all_decoded)}")
        print(f"  BTC-specific trades: {len(btc_trades)}")
        print(f"  Non-BTC trades (all other markets): {len(all_decoded) - len(btc_trades)}")

        # ── Step 5: Save trades ──────────────────────────────────────
        print(f"\n--- Step 5: Saving trades ---")

        if btc_trades:
            trades_df = pl.DataFrame(btc_trades)
            if "timestamp" in trades_df.columns:
                trades_df = trades_df.with_columns(
                    pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="s")
                    .cast(pl.Datetime("us"))
                    .alias("datetime")
                )
            trades_file = f"btc_5min_trades_{target_date}.parquet"
            trades_df.write_parquet(trades_file)
            print(f"  Saved {len(btc_trades)} BTC trades to {trades_file}")
            print(f"  DataFrame: {trades_df.shape}")
            print(f"  Columns: {trades_df.columns}")
        else:
            print(f"  No BTC trades found!")

        # Also save all trades (not just BTC) for future use
        if all_decoded:
            all_df = pl.DataFrame(all_decoded)
            if "timestamp" in all_df.columns:
                all_df = all_df.with_columns(
                    pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="s")
                    .cast(pl.Datetime("us"))
                    .alias("datetime")
                )
            all_file = f"all_trades_{target_date}.parquet"
            all_df.write_parquet(all_file)
            print(f"  Also saved {len(all_decoded)} total trades to {all_file}")

        # ── Summary ──────────────────────────────────────────────────
        print(f"\n--- Summary ---")

        if btc_trades:
            btc_df = pl.DataFrame(btc_trades)
            if "datetime" in btc_df.columns:
                print(f"  Time range: {btc_df['datetime'].min()} -> {btc_df['datetime'].max()}")
            if "price" in btc_df.columns:
                print(f"  Price range: {btc_df['price'].min():.4f} -> {btc_df['price'].max():.4f}")
            if "amount_usd" in btc_df.columns:
                print(f"  Total BTC volume: ${btc_df['amount_usd'].sum():,.2f}")
            if "side" in btc_df.columns:
                print(f"  Side counts:\n{btc_df['side'].value_counts()}")
            if "outcome" in btc_df.columns:
                print(f"  Outcome counts:\n{btc_df['outcome'].value_counts()}")
            if "_event_id" in btc_df.columns:
                per_event = btc_df.group_by("_event_id").len().sort("_event_id")
                print(
                    f"  Trades per event: "
                    f"min={per_event['len'].min()}, "
                    f"max={per_event['len'].max()}, "
                    f"median={per_event['len'].median()}"
                )

        print(f"\n  Data source: SQD Portal (free, no API key)")
        print(f"\n{'=' * 60}")
        print(f"  Done! Files saved:")
        print(f"    {events_file}")
        if btc_trades:
            print(f"    btc_5min_trades_{target_date}.parquet")
        if all_decoded:
            print(f"    all_trades_{target_date}.parquet")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
