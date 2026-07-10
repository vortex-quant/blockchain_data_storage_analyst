"""Test SQD Portal: fetch OrderFilled logs, decode V2 events, measure throughput."""

import json
import time
from datetime import UTC, datetime

import niquests

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


def decode_v2_order_filled(log: dict, block_ts: int) -> dict | None:
    """Decode a V2 OrderFilled event from SQD Portal log format.

    V2 OrderFilled event signature:
        OrderFilled(
            bytes32 orderHash,
            address maker,
            address taker,
            uint256 side,        // 0=BUY, 1=SELL (was makerAssetId in V1)
            uint256 tokenId,     // (was takerAssetId in V1)
            uint256 makerAmountFilled,
            uint256 takerAmountFilled,
            uint256 fee,
            address builder,
            bytes32 metadata
        )

    Topics: [event_sig, orderHash, maker, taker]
    Data:   [side, tokenId, makerAmountFilled, takerAmountFilled, fee, builder, metadata]
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

    side_val = fields[0]       # 0=BUY, 1=SELL
    token_id = fields[1]       # ERC1155 token ID
    maker_amount = fields[2]   # USDC (6 decimals) or tokens
    taker_amount = fields[3]   # tokens or USDC
    fee = fields[4]

    # Determine direction and calculate price/size
    # side=0 (BUY): maker gives USDC, receives tokens
    #   makerAmount = USDC, takerAmount = shares
    # side=1 (SELL): maker gives tokens, receives USDC
    #   makerAmount = shares, takerAmount = USDC
    if side_val == 0:
        # BUY: makerAmount=USDC, takerAmount=shares
        amount_usd = maker_amount / 1e6
        shares = taker_amount / 1e6
        price = maker_amount / taker_amount if taker_amount > 0 else 0.0
    elif side_val == 1:
        # SELL: makerAmount=shares, takerAmount=USDC
        shares = maker_amount / 1e6
        amount_usd = taker_amount / 1e6
        price = taker_amount / maker_amount if maker_amount > 0 else 0.0
    else:
        return None

    order_hash = topics[1]
    maker_addr = "0x" + topics[2][26:]
    taker_addr = "0x" + topics[3][26:]

    return {
        "block_number": None,  # set by caller
        "block_time": datetime.fromtimestamp(block_ts, tz=UTC).isoformat(),
        "timestamp": block_ts,
        "tx_hash": log.get("transactionHash"),
        "log_index": log.get("logIndex"),
        "order_hash": order_hash,
        "maker": maker_addr,
        "taker": taker_addr,
        "side": "BUY" if side_val == 0 else "SELL",
        "token_id": str(token_id),
        "amount_usd": amount_usd,
        "shares": shares,
        "price": price,
        "maker_amount_raw": maker_amount,
        "taker_amount_raw": taker_amount,
        "fee": fee,
    }


def fetch_sqd_logs(
    client: niquests.Session,
    from_block: int,
    to_block: int,
) -> list[tuple[dict, int, int]]:
    """Fetch logs from SQD Portal. Returns list of (log, block_number, block_ts)."""
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

    resp = client.post(SQD_URL, json=payload, timeout=120.0)
    resp.raise_for_status()

    results = []
    for line in resp.text.strip().split("\n"):
        if not line:
            continue
        obj = json.loads(line)
        if "header" not in obj:
            continue
        block_num = obj["header"]["number"]
        block_ts = obj["header"]["timestamp"]
        for log in obj.get("logs", []):
            results.append((log, block_num, block_ts))
    return results


def fetch_all_logs_paginated(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    max_per_request: int = 10000,
) -> list[tuple[dict, int, int]]:
    """Fetch all logs in a block range, handling SQD Portal pagination."""
    all_logs = []
    current = start_block

    while current <= end_block:
        to_block = min(current + max_per_request - 1, end_block)
        logs = fetch_sqd_logs(client, current, to_block)

        if not logs:
            print(f"  Blocks {current}-{to_block}: 0 logs (empty)")
            current = to_block + 1
            continue

        all_logs.extend(logs)
        last_block = logs[-1][1]
        print(
            f"  Blocks {current}-{to_block}: {len(logs)} logs "
            f"(last actual block: {last_block}, total: {len(all_logs)})"
        )
        current = last_block + 1
        time.sleep(0.1)  # rate limit courtesy

    return all_logs


def main():
    print("=" * 60)
    print("  SQD Portal Test — OrderFilled V2 Events")
    print("=" * 60)

    with niquests.Session() as client:
        # Test 1: Fetch a small range and decode one log
        print("\n--- Test 1: Small range, decode first log ---")
        logs = fetch_sqd_logs(client, 89885600, 89886490)
        print(f"  Fetched {len(logs)} logs from 890 blocks")

        if logs:
            log, block_num, block_ts = logs[0]
            print(f"\n  First log (block {block_num}):")
            print(f"    logIndex: {log['logIndex']}")
            print(f"    txHash: {log['transactionHash']}")
            print(f"    topics: {json.dumps(log['topics'], indent=6)}")

            data = log["data"][2:] if log["data"].startswith("0x") else log["data"]
            fields = [int(data[i : i + 64], 16) for i in range(0, len(data), 64)]
            print(f"    data fields ({len(fields)}):")
            for i, f in enumerate(fields):
                print(f"      [{i}] = {f} (hex: {hex(f)})")

            decoded = decode_v2_order_filled(log, block_ts)
            decoded["block_number"] = block_num
            print(f"\n  Decoded:")
            print(json.dumps(decoded, indent=4))

            # Check a few more to see side distribution
            print(f"\n  Side distribution (first 100 logs):")
            sides = {"BUY": 0, "SELL": 0}
            for log, bn, bts in logs[:100]:
                d = decode_v2_order_filled(log, bts)
                if d:
                    sides[d["side"]] = sides.get(d["side"], 0) + 1
            print(f"    {sides}")

        # Test 2: Fetch ~2 hours and count total
        print("\n--- Test 2: ~2 hour range (6400 blocks) ---")
        t0 = time.time()
        logs = fetch_all_logs_paginated(client, 89885600, 89892000)
        t1 = time.time()
        print(f"  Total logs: {len(logs)} in {t1 - t0:.1f}s")

        if logs:
            first_ts = logs[0][2]
            last_ts = logs[-1][2]
            print(
                f"  Time range: {datetime.fromtimestamp(first_ts, tz=UTC)} "
                f"-> {datetime.fromtimestamp(last_ts, tz=UTC)}"
            )

            # Decode all and show stats
            decoded = []
            for log, bn, bts in logs:
                d = decode_v2_order_filled(log, bts)
                if d:
                    d["block_number"] = bn
                    decoded.append(d)

            print(f"  Decoded: {len(decoded)} trades")
            if decoded:
                buy_count = sum(1 for d in decoded if d["side"] == "BUY")
                sell_count = sum(1 for d in decoded if d["side"] == "SELL")
                total_usd = sum(d["amount_usd"] for d in decoded)
                print(f"  BUY: {buy_count}, SELL: {sell_count}")
                print(f"  Total volume: ${total_usd:,.2f}")
                print(f"  Unique token IDs: {len({d['token_id'] for d in decoded})}")
                print(f"  Unique tx hashes: {len({d['tx_hash'] for d in decoded})}")

        # Test 3: Estimate full day
        print("\n--- Test 3: Estimate full day ---")
        # 1 day = ~57600 blocks at 1.5s/block
        # But SQD returns ~890 blocks per request, so ~65 requests
        blocks_per_day = 57600
        est_requests = (blocks_per_day + 9999) // 10000
        est_time = est_requests * 6  # ~6s per request
        est_logs = len(logs) * (blocks_per_day / 6400) if logs else 0
        print(f"  Estimated requests for 1 day: {est_requests}")
        print(f"  Estimated time: {est_time}s ({est_time / 60:.1f} min)")
        print(f"  Estimated total logs: {est_logs:,.0f}")

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
