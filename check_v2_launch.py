"""Find V2 launch by scanning from known recent blocks backward."""

import niquests
import orjson
from datetime import UTC, datetime

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

client = niquests.Session()

def check_range(from_block: int, to_block: int) -> tuple[bool, int | None, int | None]:
    payload = {
        "type": "evm",
        "fromBlock": from_block,
        "toBlock": to_block,
        "fields": {
            "block": {"number": True, "timestamp": True},
            "log": {"address": True, "topics": True, "data": True,
                    "transactionHash": True, "logIndex": True},
        },
        "logs": [{"address": [EXCHANGE_V2], "topic0": [ORDER_FILLED_TOPIC]}],
    }
    resp = client.post(SQD_URL, json=payload, timeout=120.0)
    resp.raise_for_status()
    for line in resp.text.strip().split("\n"):
        if not line:
            continue
        try:
            obj = orjson.loads(line)
        except Exception:
            continue
        if "header" not in obj:
            continue
        if obj.get("logs"):
            return True, obj["header"]["number"], obj["header"]["timestamp"]
    return False, None, None

test_ranges = [
    (89885600, "July 2026 (known)"),
    (80000000, "~May 2026"),
    (70000000, "~Jan 2026"),
    (65000000, "~Sep 2025"),
    (60000000, "~May 2025"),
    (58000000, "~Jan 2025"),
    (55000000, "~Nov 2024"),
    (50000000, "~Sep 2024"),
    (45000000, "~May 2024"),
    (40000000, "~Jan 2024"),
    (35000000, "~Jul 2023"),
    (30000000, "~Jan 2023"),
]

print("Scanning for V2 OrderFilled logs at various block heights:\n")
results = []
for fb, label in test_ranges:
    found, first_block, first_ts = check_range(fb, fb + 10000)
    if found and first_ts:
        dt = datetime.fromtimestamp(first_ts, tz=UTC)
        print(f"  {label:>20} | block {fb:>10}: FOUND (block {first_block}, {dt})")
        results.append((fb, True, first_ts))
    else:
        print(f"  {label:>20} | block {fb:>10}: none")
        results.append((fb, False, None))

print("\n\n--- Binary search for exact launch ---")
earliest_found = None
latest_not_found = None
for fb, found, ts in results:
    if found:
        if earliest_found is None or fb < earliest_found:
            earliest_found = fb
    else:
        if latest_not_found is None or fb > latest_not_found:
            latest_not_found = fb

if earliest_found and latest_not_found and latest_not_found < earliest_found:
    print(f"  Boundary: no logs at {latest_not_found}, logs at {earliest_found}")
    lo = latest_not_found
    hi = earliest_found
    while hi - lo > 1000:
        mid = (lo + hi) // 2
        found, _, _ = check_range(lo, mid)
        if found:
            hi = mid
        else:
            lo = mid
        print(f"  Range: {lo} - {hi} (gap: {hi - lo})")
    found, first_block, first_ts = check_range(lo, hi)
    if first_block:
        print(f"\n  FIRST V2 OrderFilled log:")
        print(f"    Block: {first_block}")
        print(f"    Timestamp: {datetime.fromtimestamp(first_ts, tz=UTC)}")
else:
    print(f"  earliest_found={earliest_found}, latest_not_found={latest_not_found}")
    print(f"  Could not find clear boundary")

client.close()
