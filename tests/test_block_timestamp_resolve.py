"""Test SQD Portal timestamp-to-block resolution across Polygon history.

Verifies that resolve_block_range returns correct block numbers for:
  - Historical dates (near Polygon genesis, mid-history)
  - Recent dates (near current head)
  - Boundary correctness (block timestamp >= requested timestamp)

Run:  uv run --no-sync python tests/test_block_timestamp_resolve.py
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import niquests
import orjson

from poly_data_storage.block_timestamp_resolve import (
    _sqd_timestamp_to_block,
    resolve_block_range,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_block_timestamp(client: niquests.Session, block_num: int) -> int:
    """Fetch a block's timestamp from SQD Portal stream endpoint."""
    payload = {
        "type": "evm",
        "fromBlock": block_num,
        "toBlock": block_num,
        "fields": {"block": {"number": True, "timestamp": True}},
    }
    resp = client.post(
        "https://portal.sqd.dev/datasets/polygon-mainnet/stream",
        json=payload,
        timeout=30.0,
    )
    resp.raise_for_status()
    for line in resp.iter_lines():
        if not line:
            continue
        obj = orjson.loads(line)
        if "header" in obj:
            return obj["header"]["timestamp"]
    raise ValueError(f"Block {block_num} not found in SQD response")


def _verify_block_at_or_after(
    client: niquests.Session, target_ts: int, block_num: int
) -> bool:
    """Verify the returned block's timestamp is >= target_ts,
    and the previous block's timestamp is < target_ts."""
    block_ts = _get_block_timestamp(client, block_num)
    if block_ts < target_ts:
        print(f"    FAIL: block {block_num} ts={block_ts} < target={target_ts}")
        return False

    # Check previous block
    prev_ts = _get_block_timestamp(client, block_num - 1)
    if prev_ts >= target_ts:
        print(
            f"    FAIL: prev block {block_num - 1} ts={prev_ts} >= target={target_ts}"
        )
        return False

    print(
        f"    OK: block {block_num} ts={block_ts} >= target={target_ts}, "
        f"prev block {block_num - 1} ts={prev_ts} < target"
    )
    return True


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_single_timestamp(client: niquests.Session, label: str, target_ts: int) -> bool:
    """Test _sqd_timestamp_to_block for a single timestamp."""
    print(
        f"\n  [{label}] target_ts={target_ts} ({datetime.fromtimestamp(target_ts, tz=UTC).isoformat()})"
    )
    try:
        block_num = _sqd_timestamp_to_block(client, target_ts)
        print(f"    Resolved to block {block_num}")
        return _verify_block_at_or_after(client, target_ts, block_num)
    except Exception as exc:
        print(f"    ERROR: {exc}")
        return False


def test_day_resolution(client: niquests.Session, label: str, date_str: str) -> bool:
    """Test resolve_block_range for a full UTC day."""
    print(f"\n  [{label}] date={date_str}")
    try:
        t0 = time.time()
        start_block, end_block, day_start_ts, day_end_ts = resolve_block_range(
            client, date_str
        )
        elapsed = time.time() - t0
        print(
            f"    Range: {start_block}-{end_block} ({end_block - start_block} blocks) in {elapsed:.1f}s"
        )

        # Verify start block
        ok1 = _verify_block_at_or_after(client, day_start_ts, start_block)
        # Verify end block
        ok2 = _verify_block_at_or_after(client, day_end_ts, end_block)

        # Sanity: end > start
        if end_block <= start_block:
            print(f"    FAIL: end_block ({end_block}) <= start_block ({start_block})")
            return False

        return ok1 and ok2
    except Exception as exc:
        print(f"    ERROR: {exc}")
        return False


def main():
    print("=" * 70)
    print("  SQD Portal Timestamp Resolution Test")
    print("  Verifies exact block lookup from genesis to head")
    print("=" * 70)

    results: list[tuple[str, bool]] = []

    with niquests.Session() as client:
        # ── Single timestamp tests ──────────────────────────────────────────

        # Near Polygon genesis (June 2020, genesis ~Jan 2020)
        results.append(
            (
                "genesis_2020_06_01",
                test_single_timestamp(
                    client,
                    "Historical: 2020-06-01",
                    int(datetime(2020, 6, 1, tzinfo=UTC).timestamp()),
                ),
            )
        )

        # Mid-history 2023
        results.append(
            (
                "mid_2023_07_01",
                test_single_timestamp(
                    client,
                    "Historical: 2023-07-01",
                    int(datetime(2023, 7, 1, tzinfo=UTC).timestamp()),
                ),
            )
        )

        # Mid-history 2024
        results.append(
            (
                "mid_2024_12_15",
                test_single_timestamp(
                    client,
                    "Historical: 2024-12-15",
                    int(datetime(2024, 12, 15, tzinfo=UTC).timestamp()),
                ),
            )
        )

        # Recent: yesterday
        yesterday = datetime.now(tz=UTC) - timedelta(days=1)
        results.append(
            (
                "recent_yesterday",
                test_single_timestamp(
                    client,
                    "Recent: yesterday",
                    int(
                        yesterday.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).timestamp()
                    ),
                ),
            )
        )

        # Near-current: 1 hour ago
        one_hour_ago = datetime.now(tz=UTC) - timedelta(hours=1)
        results.append(
            (
                "near_current_1h",
                test_single_timestamp(
                    client,
                    "Near-current: 1h ago",
                    int(one_hour_ago.timestamp()),
                ),
            )
        )

        # ── Full day resolution tests ───────────────────────────────────────

        # Historical day
        results.append(
            (
                "day_2023_01_15",
                test_day_resolution(client, "Historical day", "2023-01-15"),
            )
        )

        # Mid-history day
        results.append(
            (
                "day_2024_06_20",
                test_day_resolution(client, "Mid-history day", "2024-06-20"),
            )
        )

        # Recent day (yesterday)
        results.append(
            (
                "day_recent",
                test_day_resolution(
                    client, "Recent day", yesterday.strftime("%Y-%m-%d")
                ),
            )
        )

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print("  RESULTS")
    print(f"{'=' * 70}")
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"  {name:30s} {status}")
        if not passed:
            all_pass = False

    print(f"\n  {'ALL TESTS PASSED' if all_pass else 'SOME TESTS FAILED'}")  # noqa: F541
    print(f"{'=' * 70}")

    if not all_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
