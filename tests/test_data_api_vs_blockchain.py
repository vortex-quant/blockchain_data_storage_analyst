"""Compare Polymarket Data API trades with blockchain OrderFilled data.

Cross-checks the on-chain SQD Portal data (polymarket_orders_YYYY_MM_DD.parquet)
against Polymarket's own Data API trades endpoint (data-api.polymarket.com/trades).

The Data API:
  - Returns trades ordered by timestamp descending (most recent first)
  - Has a max offset of 3000 (error: "max historical activity offset of 3000 exceeded")
  - With limit=500, max retrievable trades per query = 3500
  - Does NOT support start/end time filtering (params are ignored)
  - Supports filtering by `market` (conditionId) or `eventId`
  - takerOnly=false returns both maker and taker trades (2 per fill)

The blockchain data (SQD Portal):
  - Contains every OrderFilled event (1 per fill)
  - No cap, no offset limit
  - Complete and authoritative

Comparison strategy:
  1. Select sample markets across volume tiers (low, medium, high)
  2. For each market, fetch all Data API trades (capped at 3500)
  3. Filter both datasets to the same token_ids and UTC day
  4. Verify: Data API ⊆ Blockchain (all Data API tx_hashes present in blockchain)
  5. For low-volume markets: expect exact 1:1 match
  6. For high-volume markets: expect Data API to be a capped subset

Run:
  uv run --no-sync python tests/test_data_api_vs_blockchain.py
"""

from __future__ import annotations

import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import niquests
import orjson
import pyarrow.compute as pc
import pyarrow.parquet as pq

# ── Constants ──────────────────────────────────────────────────────────────────

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

DATE_STR = "2026-07-12"
PARQUET_PATH = (
    Path(__file__).resolve().parent.parent
    / "dataset"
    / f"polymarket_orders_{DATE_STR.replace('-', '_')}.parquet"
)

DAY_START = int(datetime(2026, 7, 12, tzinfo=UTC).timestamp())
DAY_END = DAY_START + 86_400

DATA_API_LIMIT = 500
DATA_API_MAX_OFFSET = 3000
DATA_API_DELAY = 0.15
GAMMA_DELAY = 0.2
GAMMA_BATCH_SIZE = 20  # clob_token_ids per request

# Volume tiers for sampling
LOW_VOL_MIN, LOW_VOL_MAX = 10, 500  # should be fully fetchable
MED_VOL_MIN, MED_VOL_MAX = 500, 3000  # should be fully fetchable
HIGH_VOL_MIN = 3000  # will hit Data API cap

LOW_VOL_SAMPLE = 5
MED_VOL_SAMPLE = 3
HIGH_VOL_SAMPLE = 2


# ── Data API fetching ──────────────────────────────────────────────────────────


def fetch_data_api_trades(
    client: niquests.Session,
    condition_id: str,
) -> list[dict]:
    """Fetch all trades from Data API for a specific market (conditionId).

    Returns up to 3500 trades (offset cap 3000 + limit 500).
    Uses takerOnly=false to get both maker and taker trades.
    """
    all_trades: list[dict] = []
    offset = 0

    while offset <= DATA_API_MAX_OFFSET:
        for attempt in range(5):
            try:
                resp = client.get(
                    f"{DATA_API}/trades",
                    params={
                        "takerOnly": "false",
                        "limit": DATA_API_LIMIT,
                        "offset": offset,
                        "market": condition_id,
                    },
                    timeout=30.0,
                )
                if resp.status_code == 400:
                    # Offset cap exceeded
                    return all_trades
                resp.raise_for_status()
                data = orjson.loads(resp.content)
                break
            except Exception as exc:
                if attempt < 4:
                    delay = 1.0 * (2**attempt)
                    print(f"      Retry {attempt + 1}/5 after {delay}s: {exc}")
                    time.sleep(delay)
                else:
                    raise

        if not isinstance(data, list) or not data:
            break

        all_trades.extend(data)

        if len(data) < DATA_API_LIMIT:
            break

        offset += DATA_API_LIMIT
        time.sleep(DATA_API_DELAY)

    return all_trades


# ── Gamma API: token_id → conditionId mapping ─────────────────────────────────


def lookup_condition_ids(
    client: niquests.Session,
    token_ids: list[str],
) -> dict[str, str]:
    """Look up conditionId for each token_id via Gamma API /markets endpoint.

    Returns mapping: token_id -> conditionId
    """
    mapping: dict[str, str] = {}

    for tid in token_ids:
        for attempt in range(5):
            try:
                resp = client.get(
                    f"{GAMMA_API}/markets",
                    params={"clob_token_ids": tid, "limit": 5},
                    timeout=30.0,
                )
                resp.raise_for_status()
                data = orjson.loads(resp.content)
                break
            except Exception as exc:
                if attempt < 4:
                    delay = 1.0 * (2**attempt)
                    time.sleep(delay)
                else:
                    print(f"  WARNING: Gamma lookup failed for {tid}: {exc}")
                    data = []

        if isinstance(data, list):
            for market in data:
                cid = market.get("conditionId")
                tokens = market.get("clobTokenIds", [])
                if isinstance(tokens, str):
                    import json

                    tokens = json.loads(tokens)
                if isinstance(tokens, list):
                    for t in tokens:
                        mapping[str(t)] = cid

        time.sleep(GAMMA_DELAY)

    return mapping


# ── Blockchain data helpers ────────────────────────────────────────────────────


def load_blockchain_data() -> pq.Table:
    """Load the blockchain parquet file."""
    if not PARQUET_PATH.exists():
        print(f"ERROR: Parquet file not found: {PARQUET_PATH}")
        sys.exit(1)

    print(f"Loading blockchain data from {PARQUET_PATH}...")
    t = pq.read_table(PARQUET_PATH)
    print(f"  Rows: {t.num_rows:,}")
    print(f"  Unique token_ids: {pc.count_distinct(t.column('token_id')).as_py():,}")
    print(f"  Unique tx_hashes: {pc.count_distinct(t.column('tx_hash')).as_py():,}")

    ts_col = t.column("timestamp")
    ts_min = pc.min(ts_col).as_py()
    ts_max = pc.max(ts_col).as_py()
    print(
        f"  Timestamp range: {datetime.fromtimestamp(ts_min, tz=UTC)} - {datetime.fromtimestamp(ts_max, tz=UTC)}"
    )

    return t


def get_token_counts(bc_table: pq.Table) -> Counter:
    """Get per-token_id trade counts from blockchain data."""
    return Counter(bc_table.column("token_id").to_pylist())


def filter_blockchain_by_tokens(bc_table: pq.Table, token_ids: list[str]) -> pq.Table:
    """Filter blockchain data to specific token_ids."""
    token_col = bc_table.column("token_id")
    masks = [pc.equal(token_col, tid) for tid in token_ids]
    mask = masks[0]
    for m in masks[1:]:
        mask = pc.or_(mask, m)
    return bc_table.filter(mask)


# ── Comparison logic ───────────────────────────────────────────────────────────


def compare_market(
    client: niquests.Session,
    bc_table: pq.Table,
    token_ids: list[str],
    condition_id: str,
    market_label: str,
) -> dict:
    """Compare Data API vs blockchain data for a single market.

    Returns a result dict with comparison metrics.
    """
    print(f"\n  [{market_label}] conditionId={condition_id}")
    print(f"  token_ids: {len(token_ids)}")

    # Filter blockchain data
    bc_filtered = filter_blockchain_by_tokens(bc_table, token_ids)
    bc_count = bc_filtered.num_rows
    bc_tx_hashes = set(bc_filtered.column("tx_hash").to_pylist())
    print(f"  Blockchain: {bc_count:,} fills, {len(bc_tx_hashes):,} unique tx_hashes")

    # Fetch Data API trades
    print("  Fetching Data API trades...")
    api_trades = fetch_data_api_trades(client, condition_id)
    print(f"  Data API: {len(api_trades)} trades (takerOnly=false)")

    # Filter Data API trades to the target day
    api_in_day = [t for t in api_trades if DAY_START <= t.get("timestamp", 0) < DAY_END]
    api_tx_hashes = {t.get("transactionHash") for t in api_in_day}
    print(
        f"  Data API (on {DATE_STR}): {len(api_in_day)} trades, {len(api_tx_hashes)} unique tx_hashes"
    )

    # Filter Data API trades to the specific token_ids
    token_id_set = set(token_ids)
    api_matching_tokens = [
        t for t in api_in_day if str(t.get("asset", "")) in token_id_set
    ]
    api_matching_tx = {t.get("transactionHash") for t in api_matching_tokens}
    print(
        f"  Data API (matching tokens): {len(api_matching_tokens)} trades, {len(api_matching_tx)} tx_hashes"
    )

    # Check: all Data API tx_hashes should be in blockchain
    api_in_bc = api_matching_tx & bc_tx_hashes
    api_not_in_bc = api_matching_tx - bc_tx_hashes
    bc_not_in_api = bc_tx_hashes - api_matching_tx

    print("\n  --- Tx Hash Cross-Check ---")
    print(
        f"  Data API tx_hashes found in blockchain:    {len(api_in_bc)} / {len(api_matching_tx)}"
    )
    print(f"  Data API tx_hashes NOT in blockchain:      {len(api_not_in_bc)}")
    print(f"  Blockchain tx_hashes NOT in Data API:      {len(bc_not_in_api)}")

    if api_not_in_bc:
        print(
            f"  WARNING: {len(api_not_in_bc)} Data API trades not found in blockchain!"
        )
        for tx in list(api_not_in_bc)[:3]:
            print(f"    {tx}")

    # Determine if Data API was capped
    api_capped = len(api_trades) >= 3500

    # For non-capped markets, expect exact tx_hash match
    exact_match = (
        (len(api_not_in_bc) == 0) and (len(bc_not_in_api) == 0)
        if not api_capped
        else None
    )

    # Verify trade details for matching tx_hashes
    detail_matches = 0
    detail_mismatches = 0

    if api_in_bc:
        # Build blockchain lookup: tx_hash -> list of fills
        bc_by_tx: dict[str, list[dict]] = {}
        bc_rows = bc_filtered.to_pylist()
        for row in bc_rows:
            tx = row["tx_hash"]
            bc_by_tx.setdefault(tx, []).append(row)

        for api_trade in api_matching_tokens:
            tx = api_trade.get("transactionHash")
            if tx not in bc_by_tx:
                continue

            bc_fills = bc_by_tx[tx]
            api_side = api_trade.get("side")
            api_price = round(api_trade.get("price", 0), 6)
            api_size = round(api_trade.get("size", 0), 6)
            api_asset = str(api_trade.get("asset", ""))

            # Find matching fill by token_id and side
            for fill in bc_fills:
                if fill["token_id"] == api_asset and fill["side"] == api_side:
                    bc_price = round(fill["price"], 6)
                    bc_shares = round(fill["shares"], 6)
                    if (
                        abs(bc_price - api_price) < 0.001
                        and abs(bc_shares - api_size) < 0.01
                    ):
                        detail_matches += 1
                    else:
                        detail_mismatches += 1
                        if detail_mismatches <= 3:
                            print(
                                f"    DETAIL MISMATCH: tx={tx[:16]}... "
                                f"side={api_side} "
                                f"price API={api_price} BC={bc_price} "
                                f"size API={api_size} BC={bc_shares}"
                            )
                    break

    print("\n  --- Trade Detail Verification ---")
    print(f"  Detail matches:    {detail_matches}")
    print(f"  Detail mismatches: {detail_mismatches}")

    result = {
        "label": market_label,
        "condition_id": condition_id,
        "bc_count": bc_count,
        "bc_tx_hashes": len(bc_tx_hashes),
        "api_total": len(api_trades),
        "api_in_day": len(api_in_day),
        "api_matching_tokens": len(api_matching_tokens),
        "api_tx_hashes": len(api_matching_tx),
        "api_in_bc": len(api_in_bc),
        "api_not_in_bc": len(api_not_in_bc),
        "bc_not_in_api": len(bc_not_in_api),
        "api_capped": api_capped,
        "exact_match": exact_match,
        "detail_matches": detail_matches,
        "detail_mismatches": detail_mismatches,
        "pass": len(api_not_in_bc) == 0,
    }

    return result


# ── Main ───────────────────────────────────────────────────────────────────────


def main():
    print("=" * 70)
    print(f"  Data API vs Blockchain Comparison Test — {DATE_STR}")
    print("=" * 70)

    # Load blockchain data
    bc_table = load_blockchain_data()

    # Get per-token trade counts
    token_counts = get_token_counts(bc_table)

    # Select samples from each volume tier
    low_vol = sorted(
        [
            (tid, cnt)
            for tid, cnt in token_counts.items()
            if LOW_VOL_MIN <= cnt <= LOW_VOL_MAX
        ],
        key=lambda x: x[1],
    )
    med_vol = sorted(
        [
            (tid, cnt)
            for tid, cnt in token_counts.items()
            if MED_VOL_MIN < cnt <= MED_VOL_MAX
        ],
        key=lambda x: x[1],
    )
    high_vol = sorted(
        [(tid, cnt) for tid, cnt in token_counts.items() if cnt > HIGH_VOL_MIN],
        key=lambda x: x[1],
    )

    print("\n  Volume tiers:")
    print(
        f"    Low ({LOW_VOL_MIN}-{LOW_VOL_MAX} trades):   {len(low_vol):,} tokens, sampling {min(LOW_VOL_SAMPLE, len(low_vol))}"
    )
    print(
        f"    Medium ({MED_VOL_MIN + 1}-{MED_VOL_MAX} trades): {len(med_vol):,} tokens, sampling {min(MED_VOL_SAMPLE, len(med_vol))}"
    )
    print(
        f"    High (>{HIGH_VOL_MIN} trades):     {len(high_vol):,} tokens, sampling {min(HIGH_VOL_SAMPLE, len(high_vol))}"
    )

    # Select samples (spread across the tier)
    def spread_sample(items, n):
        if len(items) <= n:
            return items
        step = len(items) / n
        return [items[int(i * step)] for i in range(n)]

    low_sample = spread_sample(low_vol, LOW_VOL_SAMPLE)
    med_sample = spread_sample(med_vol, MED_VOL_SAMPLE)
    high_sample = spread_sample(high_vol, HIGH_VOL_SAMPLE)

    # Collect all sampled token_ids
    all_sampled = low_sample + med_sample + high_sample
    all_token_ids = [tid for tid, _ in all_sampled]

    # Also add a known BTC 5-min market for high-volume capped testing.
    # These token_ids come from the first BTC 5-min event on 2026-07-12
    # (slug: btc-updown-5m-1783814400, conditionId: 0x4c2b0d7b...)
    btc_5min_tokens = [
        "41590868698844731693919196382535076142473982008228244562680705896443905539787",
        "90578874222773312659824310952721312310472669302390991958940668452593308669664",
    ]
    btc_5min_cid = "0x4c2b0d7b95c08878580a14702c64088329e3604ab8db58984d548ddf705157bb"

    print(
        f"\n  Total sampled token_ids: {len(all_token_ids)} + {len(btc_5min_tokens)} BTC 5-min = {len(all_token_ids) + len(btc_5min_tokens)}"
    )

    # Look up conditionIds from Gamma API
    print("\n--- Looking up conditionIds from Gamma API ---")
    with niquests.Session() as client:
        tid_to_cid = lookup_condition_ids(client, all_token_ids)

    print(
        f"  Mapped {len(tid_to_cid)} / {len(all_token_ids)} token_ids to conditionIds"
    )

    # Add known BTC 5-min mapping (no Gamma lookup needed)
    for tid in btc_5min_tokens:
        tid_to_cid[tid] = btc_5min_cid
    print(f"  After adding BTC 5-min: {len(tid_to_cid)} token_ids mapped")

    # Group token_ids by conditionId
    cid_to_tids: dict[str, list[str]] = {}
    for tid, cid in tid_to_cid.items():
        cid_to_tids.setdefault(cid, []).append(tid)

    print(f"  {len(cid_to_tids)} unique conditionIds (markets)")

    # Run comparisons
    print(f"\n{'=' * 70}")
    print("  Running comparisons")
    print(f"{'=' * 70}")

    results = []
    with niquests.Session() as client:
        for i, (cid, tids) in enumerate(cid_to_tids.items()):
            # Find the trade count for this market
            total_trades = sum(token_counts.get(tid, 0) for tid in tids)

            if total_trades <= LOW_VOL_MAX:
                tier = "LOW"
            elif total_trades <= MED_VOL_MAX:
                tier = "MED"
            else:
                tier = "HIGH"

            label = f"{tier}-vol #{i + 1} ({total_trades} BC trades)"
            result = compare_market(client, bc_table, tids, cid, label)
            results.append(result)

    # Final summary
    print(f"\n{'=' * 70}")
    print("  FINAL SUMMARY")
    print(f"{'=' * 70}")
    print(
        f"  {'Market':<35} {'BC':>8} {'API':>8} {'API∩BC':>8} {'API∉BC':>8} {'BC∉API':>8} {'Capped':>7} {'Pass':>6}"
    )
    print(
        f"  {'-' * 35} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 7} {'-' * 6}"
    )

    all_pass = True
    for r in results:
        status = "PASS" if r["pass"] else "FAIL"
        capped = "YES" if r["api_capped"] else "no"
        print(
            f"  {r['label']:<35} "
            f"{r['bc_count']:>8,} "
            f"{r['api_matching_tokens']:>8,} "
            f"{r['api_in_bc']:>8,} "
            f"{r['api_not_in_bc']:>8,} "
            f"{r['bc_not_in_api']:>8,} "
            f"{capped:>7} "
            f"{status:>6}"
        )
        if not r["pass"]:
            all_pass = False

    print(f"\n  {'ALL TESTS PASSED' if all_pass else 'SOME TESTS FAILED'}")

    # Detailed analysis
    print(f"\n{'=' * 70}")
    print("  ANALYSIS")
    print(f"{'=' * 70}")

    total_bc = sum(r["bc_count"] for r in results)
    total_api = sum(r["api_matching_tokens"] for r in results)
    total_api_in_bc = sum(r["api_in_bc"] for r in results)
    total_api_not_bc = sum(r["api_not_in_bc"] for r in results)
    capped_markets = sum(1 for r in results if r["api_capped"])

    print(f"  Markets tested:           {len(results)}")
    print(f"  Capped markets (Data API): {capped_markets}")
    print(f"  Total blockchain fills:   {total_bc:,}")
    print(f"  Total Data API trades:    {total_api:,}")
    print(f"  Data API ⊆ Blockchain:    {total_api_not_bc == 0}")
    print(f"  Data API trades in BC:    {total_api_in_bc:,}")
    print(f"  Data API trades NOT in BC: {total_api_not_bc}")

    if total_api_not_bc == 0:
        print(
            "\n  CONFIRMED: All Data API trades are present in blockchain (SQD Portal) data."
        )
        print("  The blockchain data is a superset of the Data API.")
    else:
        print(
            f"\n  WARNING: {total_api_not_bc} Data API trades not found in blockchain data!"
        )

    # For non-capped markets, check exact match
    non_capped = [r for r in results if not r["api_capped"]]
    if non_capped:
        exact_matches = sum(1 for r in non_capped if r["bc_not_in_api"] == 0)
        print(f"\n  Non-capped markets: {len(non_capped)}")
        print(f"  Exact tx_hash match: {exact_matches} / {len(non_capped)}")
        if exact_matches == len(non_capped):
            print("  CONFIRMED: All non-capped markets have exact 1:1 tx_hash match!")

    print(f"\n{'=' * 70}")
    if not all_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
