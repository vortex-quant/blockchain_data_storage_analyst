"""Check if top non-BTC tokens are from ETH/SOL 5-min events or other sources."""

import json
import time
from datetime import UTC, datetime

import niquests
import polars as pl

client = niquests.Session()
GAMMA = "https://gamma-api.polymarket.com"

df = pl.read_parquet("all_trades_2026-07-09.parquet")

day_start = int(datetime(2026, 7, 9, tzinfo=UTC).timestamp())
timestamps = [day_start + i * 300 for i in range(288)]

eth_token_ids = set()
sol_token_ids = set()
btc_token_ids = set()

# BTC
btc_events = pl.read_parquet("btc_5min_events_2026-07-09.parquet")
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

# ETH and SOL
for pattern, token_set in [("eth-updown-5m-", eth_token_ids), ("sol-updown-5m-", sol_token_ids)]:
    slugs = [f"{pattern}{ts}" for ts in timestamps]
    for i in range(0, len(slugs), 20):
        batch = slugs[i : i + 20]
        resp = client.get(f"{GAMMA}/events", params={"slug": batch, "closed": "true"}, timeout=30.0)
        if resp.status_code != 200:
            continue
        data = resp.json()
        if isinstance(data, list):
            for ev in data:
                markets = ev.get("markets", [])
                if isinstance(markets, str):
                    markets = json.loads(markets)
                for m in markets:
                    tokens = m.get("clobTokenIds", [])
                    if isinstance(tokens, str):
                        tokens = json.loads(tokens)
                    for t in tokens:
                        token_set.add(str(t))
        time.sleep(0.2)

print(f"BTC token IDs: {len(btc_token_ids)}")
print(f"ETH token IDs: {len(eth_token_ids)}")
print(f"SOL token IDs: {len(sol_token_ids)}")

all_token_ids = set(df["token_id"].to_list())
eth_in_trades = eth_token_ids & all_token_ids
sol_in_trades = sol_token_ids & all_token_ids
btc_in_trades = btc_token_ids & all_token_ids
print(f"\nBTC tokens in trades: {len(btc_in_trades)} / {len(btc_token_ids)}")
print(f"ETH tokens in trades: {len(eth_in_trades)} / {len(eth_token_ids)}")
print(f"SOL tokens in trades: {len(sol_in_trades)} / {len(sol_token_ids)}")

eth_trades = df.filter(pl.col("token_id").is_in(list(eth_token_ids)))
sol_trades = df.filter(pl.col("token_id").is_in(list(sol_token_ids)))
btc_trades = df.filter(pl.col("token_id").is_in(list(btc_token_ids)))
known = btc_token_ids | eth_token_ids | sol_token_ids
unknown_trades = df.filter(~pl.col("token_id").is_in(list(known)))

print(f"\n--- Trade counts by category ---")
print(f"BTC 5-min:  {btc_trades.shape[0]:>10,} trades (${btc_trades['amount_usd'].sum():>14,.2f})")
print(f"ETH 5-min:  {eth_trades.shape[0]:>10,} trades (${eth_trades['amount_usd'].sum():>14,.2f})")
print(f"SOL 5-min:  {sol_trades.shape[0]:>10,} trades (${sol_trades['amount_usd'].sum():>14,.2f})")
print(f"UNKNOWN:    {unknown_trades.shape[0]:>10,} trades (${unknown_trades['amount_usd'].sum():>14,.2f})")
print(f"TOTAL:      {df.shape[0]:>10,} trades")

# Try more slug patterns
print(f"\n--- Trying more slug patterns ---")
more_patterns = [
    "btc-updown-1h-", "eth-updown-1h-", "sol-updown-1h-",
    "bnb-updown-5m-", "xrp-updown-5m-", "doge-updown-5m-",
    "pepe-updown-5m-", "link-updown-5m-", "sui-updown-5m-",
    "ada-updown-5m-", "avax-updown-5m-", "shib-updown-5m-",
    "ton-updown-5m-", "matic-updown-5m-", "near-updown-5m-",
    "apt-updown-5m-", "op-updown-5m-", "arb-updown-5m-",
    "hbar-updown-5m-", "ltc-updown-5m-", "bch-updown-5m-",
    "dot-updown-5m-", "atom-updown-5m-", "uni-updown-5m-",
    "aave-updown-5m-", "fil-updown-5m-", "rndr-updown-5m-",
    "wif-updown-5m-", "bonk-updown-5m-", "floki-updown-5m-",
    "trump-updown-5m-", "nfl-updown-5m-",
]

found_patterns = {}
for pattern in more_patterns:
    test_slug = f"{pattern}{timestamps[100]}"
    resp = client.get(f"{GAMMA}/events", params={"slug": [test_slug], "closed": "true"}, timeout=15.0)
    if resp.status_code == 200:
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            title = data[0].get("title", "?")
            found_patterns[pattern] = title
            print(f"  {pattern}: FOUND! Title: {title}")
    time.sleep(0.1)

# For found patterns, fetch all 288 events and collect token IDs
print(f"\n--- Fetching all events for found patterns ---\n")
all_known_tokens = btc_token_ids | eth_token_ids | sol_token_ids

for pattern, title in found_patterns.items():
    slugs = [f"{pattern}{ts}" for ts in timestamps]
    pattern_tokens = set()
    for i in range(0, len(slugs), 20):
        batch = slugs[i : i + 20]
        resp = client.get(f"{GAMMA}/events", params={"slug": batch, "closed": "true"}, timeout=30.0)
        if resp.status_code != 200:
            continue
        data = resp.json()
        if isinstance(data, list):
            for ev in data:
                markets = ev.get("markets", [])
                if isinstance(markets, str):
                    markets = json.loads(markets)
                for m in markets:
                    tokens = m.get("clobTokenIds", [])
                    if isinstance(tokens, str):
                        tokens = json.loads(tokens)
                    for t in tokens:
                        pattern_tokens.add(str(t))
        time.sleep(0.2)
    all_known_tokens |= pattern_tokens
    pattern_trades = df.filter(pl.col("token_id").is_in(list(pattern_tokens)))
    print(f"  {pattern}: {len(pattern_tokens)} tokens, {pattern_trades.shape[0]:,} trades, ${pattern_trades['amount_usd'].sum():,.2f}")

# Final breakdown
print(f"\n--- Final breakdown ---")
remaining = df.filter(~pl.col("token_id").is_in(list(all_known_tokens)))
print(f"  Identified:   {df.shape[0] - remaining.shape[0]:>10,} trades ({(df.shape[0] - remaining.shape[0]) / df.shape[0] * 100:.1f}%)")
print(f"  Unidentified: {remaining.shape[0]:>10,} trades ({remaining.shape[0] / df.shape[0] * 100:.1f}%)")

if remaining.shape[0] > 0:
    print(f"\n  Top 10 unidentified tokens:")
    rem_top = (
        remaining.group_by("token_id")
        .agg(pl.len().alias("trades"), pl.col("amount_usd").sum().alias("volume_usd"))
        .sort("trades", descending=True)
        .head(10)
    )
    print(rem_top)

client.close()
