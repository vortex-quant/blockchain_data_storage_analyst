"""Check total event count on Polymarket for a day — not just crypto 5-min."""

import time
from datetime import UTC, datetime

import niquests
import orjson
import polars as pl

GAMMA = "https://gamma-api.polymarket.com"
client = niquests.Session()

# Method 1: Fetch all closed events paginated (no date filter — Gamma's date filter is broken)
print("--- Fetching all events from Gamma (paginated) ---")
all_events: list[dict] = []
offset = 0
while offset < 50000:
    resp = client.get(
        f"{GAMMA}/events",
        params={"limit": 100, "offset": offset, "closed": "true",
                "order": "endDate", "ascending": "false"},
        timeout=30.0,
    )
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code} at offset {offset}")
        break
    data = orjson.loads(resp.content)
    if not data or not isinstance(data, list):
        break
    all_events.extend(data)
    if len(data) < 100:
        break
    offset += 100
    time.sleep(0.2)

print(f"  Total events fetched: {len(all_events)}")

# Filter to July 9 2026 by endDate
target_day = datetime(2026, 7, 9, tzinfo=UTC)
day_start = target_day.timestamp()
day_end = (target_day.replace(hour=23, minute=59, second=59)).timestamp()

july9_events = []
for ev in all_events:
    et = ev.get("endDate") or ev.get("closedTime") or ev.get("startDate")
    if et:
        ts = et.replace("Z", "+00:00") if isinstance(et, str) else et
        try:
            event_ts = datetime.fromisoformat(ts).timestamp()
            if day_start <= event_ts <= day_end + 3600:
                july9_events.append(ev)
        except Exception:
            pass

print(f"\n  Events ending on July 9 2026: {len(july9_events)}")

# Categorize
categories: dict[str, int] = {}
for ev in july9_events:
    title = ev.get("title", "?").lower()
    if "btc" in title or "bitcoin" in title:
        cat = "BTC"
    elif "ethereum" in title or "eth" in title:
        cat = "ETH"
    elif "solana" in title or "sol" in title:
        cat = "SOL"
    elif "xrp" in title:
        cat = "XRP"
    elif "bnb" in title:
        cat = "BNB"
    elif "doge" in title:
        cat = "DOGE"
    elif "up or down" in title or "updown" in title:
        cat = "Other Crypto 5-min"
    elif "nba" in title or "basketball" in title:
        cat = "NBA"
    elif "nfl" in title or "football" in title:
        cat = "NFL"
    elif "mlb" in title or "baseball" in title:
        cat = "MLB"
    elif "soccer" in title or "premier" in title or "la liga" in title:
        cat = "Soccer"
    elif "election" in title or "president" in title or "trump" in title:
        cat = "Politics"
    elif "price" in title or "hit" in title or "close above" in title:
        cat = "Crypto Price"
    else:
        cat = "Other"
    categories[cat] = categories.get(cat, 0) + 1

print(f"\n--- Event count by category (July 9) ---")
for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
    print(f"  {count:>5} events | {cat}")

# Also check: how many slug patterns exist?
print(f"\n--- Checking slug patterns ---")
day_start_ts = int(datetime(2026, 7, 9, tzinfo=UTC).timestamp())
test_ts = day_start_ts + 100 * 300  # midday

patterns_to_test = [
    "btc-updown-5m-", "eth-updown-5m-", "sol-updown-5m-",
    "xrp-updown-5m-", "bnb-updown-5m-", "doge-updown-5m-",
    "link-updown-5m-", "sui-updown-5m-", "ada-updown-5m-",
    "avax-updown-5m-", "ton-updown-5m-", "pepe-updown-5m-",
    "shib-updown-5m-", "near-updown-5m-", "apt-updown-5m-",
    "op-updown-5m-", "arb-updown-5m-", "matic-updown-5m-",
    "ltc-updown-5m-", "bch-updown-5m-", "dot-updown-5m-",
    "hbar-updown-5m-", "rndr-updown-5m-", "wif-updown-5m-",
    "bonk-updown-5m-", "floki-updown-5m-", "trump-updown-5m-",
    "pump-updown-5m-", "goat-updown-5m-", "fartcoin-updown-5m-",
    "btc-updown-1h-", "eth-updown-1h-", "sol-updown-1h-",
]

found_patterns = {}
for pattern in patterns_to_test:
    slug = f"{pattern}{test_ts}"
    resp = client.get(f"{GAMMA}/events", params={"slug": [slug], "closed": "true"}, timeout=15.0)
    if resp.status_code == 200:
        data = orjson.loads(resp.content)
        if isinstance(data, list) and len(data) > 0:
            found_patterns[pattern] = data[0].get("title", "?")
    time.sleep(0.1)

print(f"  Found {len(found_patterns)} active slug patterns:")
for pattern, title in sorted(found_patterns.items()):
    print(f"    {pattern} -> {title}")

# Count total crypto 5-min events
crypto_5min_count = len(found_patterns) * 288
print(f"\n  Crypto 5-min events: {len(found_patterns)} patterns × 288 = {crypto_5min_count}")
print(f"  Total July 9 events: {len(july9_events)}")
print(f"  Crypto 5-min % of events: {crypto_5min_count / len(july9_events) * 100:.1f}%" if july9_events else "")

client.close()
