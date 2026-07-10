"""Analyze all_trades: break down by market type, identify non-BTC markets."""

import json
import time

import niquests
import polars as pl

client = niquests.Session()
GAMMA = "https://gamma-api.polymarket.com"

df = pl.read_parquet("all_trades_2026-07-09.parquet")

print(f"Total OrderFilled logs: {df.shape[0]:,}")
print(f"Unique token IDs: {df['token_id'].n_unique():,}")
print(f"Unique tx hashes: {df['tx_hash'].n_unique():,}")
print()

# BTC token IDs
btc_events = pl.read_parquet("btc_5min_events_2026-07-09.parquet")
btc_token_ids = set()
for row in btc_events.iter_rows(named=True):
    markets = row.get("markets", [])
    if isinstance(markets, str):
        markets = json.loads(markets)
    for m in markets:
        tokens = m.get("clobTokenIds", [])
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        for t in tokens:
            btc_token_ids.add(str(t))

btc_trades = df.filter(pl.col("token_id").is_in(list(btc_token_ids)))
non_btc = df.filter(~pl.col("token_id").is_in(list(btc_token_ids)))
print(f"BTC 5-min trades: {btc_trades.shape[0]:,} ({btc_trades.shape[0]/df.shape[0]*100:.1f}%)")
print(f"Non-BTC trades:   {non_btc.shape[0]:,} ({non_btc.shape[0]/df.shape[0]*100:.1f}%)")
print(f"BTC volume: ${btc_trades['amount_usd'].sum():,.2f}")
print(f"Non-BTC volume: ${non_btc['amount_usd'].sum():,.2f}")
print()

# Why is BTC 45%? Because 288 events x 2 outcomes = 576 tokens, each with ~7400 trades
print(f"BTC unique token IDs: {len(btc_token_ids)} (288 events x 2 outcomes)")
print(f"Non-BTC unique token IDs: {non_btc['token_id'].n_unique()}")
print(f"BTC avg trades/token: {btc_trades.shape[0] / len(btc_token_ids):.0f}")
print(f"Non-BTC avg trades/token: {non_btc.shape[0] / non_btc['token_id'].n_unique():.0f}")
print()

# Non-BTC top tokens
non_btc_per_token = (
    non_btc.group_by("token_id")
    .agg(pl.len().alias("trades"), pl.col("amount_usd").sum().alias("volume_usd"))
    .sort("trades", descending=True)
)

# Fetch active + closed markets from Gamma to identify tokens
print("--- Fetching markets from Gamma API ---")
token_map: dict[str, str] = {}

for status in ["true", "false"]:
    label = "active" if status == "true" else "closed"
    offset = 0
    while offset < 10000:
        resp = client.get(
            f"{GAMMA}/markets",
            params={"limit": 100, "offset": offset, "active": status, "closed": status},
            timeout=30.0,
        )
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data or not isinstance(data, list):
            break
        for m in data:
            question = m.get("question", "?")
            tokens = m.get("clobTokenIds", [])
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            outcomes = m.get("outcomes", [])
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(tokens, list):
                for i, t in enumerate(tokens):
                    outcome = outcomes[i] if i < len(outcomes) else "?"
                    token_map[str(t)] = f"{question} :: {outcome}"
        if len(data) < 100:
            break
        offset += 100
        time.sleep(0.2)
    print(f"  {label}: fetched to offset {offset}, token_map now {len(token_map)}")

print(f"\n  Total token_map size: {len(token_map)}")

# Look up top 30 non-BTC tokens
print(f"\n--- Top 30 NON-BTC markets by trade count ---\n")
found = 0
for row in non_btc_per_token.head(30).iter_rows(named=True):
    token_id = row["token_id"]
    trades = row["trades"]
    volume = row["volume_usd"]
    name = token_map.get(token_id, "NOT FOUND")
    if name != "NOT FOUND":
        found += 1
    print(f"  trades={trades:>6}  vol=${volume:>12,.0f}  | {name[:80]}")

print(f"\n  Found: {found}/30")

# Group non-BTC by approximate market type using token_map
print(f"\n--- Non-BTC market categories (from found tokens) ---\n")
categories: dict[str, int] = {}
for row in non_btc_per_token.iter_rows(named=True):
    token_id = row["token_id"]
    trades = row["trades"]
    name = token_map.get(token_id, None)
    if name:
        # Simple categorization
        name_lower = name.lower()
        if "ethereum" in name_lower or "eth" in name_lower:
            cat = "ETH"
        elif "solana" in name_lower or "sol" in name_lower:
            cat = "SOL"
        elif "bitcoin" in name_lower or "btc" in name_lower:
            cat = "BTC (non-5min)"
        elif "mlb" in name_lower or "baseball" in name_lower:
            cat = "MLB"
        elif "nba" in name_lower or "basketball" in name_lower:
            cat = "NBA"
        elif "nfl" in name_lower or "football" in name_lower:
            cat = "NFL"
        elif "soccer" in name_lower or "premier" in name_lower:
            cat = "Soccer"
        elif "election" in name_lower or "president" in name_lower:
            cat = "Elections"
        elif "trump" in name_lower:
            cat = "Trump"
        else:
            cat = "Other"
        categories[cat] = categories.get(cat, 0) + trades

for cat, trades in sorted(categories.items(), key=lambda x: x[1], reverse=True):
    print(f"  {trades:>8,} trades | {cat}")

unidentified = non_btc.shape[0] - sum(categories.values())
print(f"  {unidentified:>8,} trades | UNIDENTIFIED (no Gamma match)")

client.close()
