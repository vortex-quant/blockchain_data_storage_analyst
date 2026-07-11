"""Block estimation helpers — Polygon RPC queries and UTC day block ranges."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import niquests

from poly_data_storage.constants import (
    ALCHEMY_API_KEY_ENV,
    ALCHEMY_POLYGON_RPC,
    BLOCK_BUFFER_BASE,
    BLOCK_BUFFER_PER_DAY,
    MAX_RETRIES,
    POLYGON_BLOCK_TIME,
    POLYGON_RPC_FALLBACKS,
    RETRY_BASE_DELAY,
)


def _load_alchemy_key() -> str | None:
    """Load Alchemy API key from keys.env in the project root."""
    # Check environment variable first
    key = os.environ.get(ALCHEMY_API_KEY_ENV)
    if key:
        return key
    # Try keys.env in project root (walk up from this file)
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        env_file = parent / "keys.env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line.startswith("alchemy_api_key="):
                    return line.split("=", 1)[1].strip()
    return None


def _get_rpc_urls() -> list[str]:
    """Build RPC URL list: Alchemy (if key available) + free fallbacks."""
    urls: list[str] = []
    key = _load_alchemy_key()
    if key:
        urls.append(ALCHEMY_POLYGON_RPC.format(key=key))
    urls.extend(POLYGON_RPC_FALLBACKS)
    return urls


def find_block_for_timestamp(
    target_ts: int,
    known_block: int,
    known_block_ts: int,
) -> int:
    """Estimate block number for a target Unix timestamp using ~2s block time."""
    delta_seconds = target_ts - known_block_ts
    delta_blocks = int(delta_seconds / POLYGON_BLOCK_TIME)
    return known_block + delta_blocks


def _rpc_call(client: niquests.Session, method: str, params: list) -> dict:
    """Try each RPC endpoint in order until one succeeds."""
    urls = _get_rpc_urls()
    last_exc = None
    for url in urls:
        try:
            resp = client.post(
                url,
                json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
                timeout=30.0,
            )
            resp.raise_for_status()
            return resp.json()["result"]
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"All {len(urls)} RPC endpoints failed: {last_exc}")


def get_latest_block_info(client: niquests.Session) -> tuple[int, int]:
    """Get latest block number and timestamp from Polygon RPC with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            block_hex = _rpc_call(client, "eth_blockNumber", [])
            block_num = int(block_hex, 16)

            result = _rpc_call(client, "eth_getBlockByNumber", [block_hex, False])
            block_ts = int(result["timestamp"], 16)

            return block_num, block_ts
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                print(f"  RPC retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                time.sleep(delay)
            else:
                raise


def estimate_block_range(
    client: niquests.Session,
    date_str: str,
) -> tuple[int, int, int, int]:
    """Estimate block range covering a UTC day.

    Uses UTC midnight boundaries calibrated against the latest block from
    Polygon RPC. The block buffer scales with the time distance from the
    latest block to account for block time variance over longer periods.

    Returns (start_block, end_block, day_start_ts, day_end_ts).
    The caller must filter by timestamp [day_start_ts, day_end_ts) before
    writing, as the block range is a deliberately wide superset.
    """
    day_start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end_dt = day_start_dt + timedelta(days=1)
    day_start_ts = int(day_start_dt.timestamp())
    day_end_ts = int(day_end_dt.timestamp())

    latest_block, latest_ts = get_latest_block_info(client)
    print(f"  Latest block: {latest_block} (ts: {datetime.fromtimestamp(latest_ts, tz=UTC)})")

    est_start = find_block_for_timestamp(day_start_ts, latest_block, latest_ts)
    est_end = find_block_for_timestamp(day_end_ts, latest_block, latest_ts)

    # Scale buffer with time distance to account for block time variance
    days_from_latest = abs(latest_ts - day_start_ts) / 86_400
    buffer = int(BLOCK_BUFFER_BASE + BLOCK_BUFFER_PER_DAY * days_from_latest)

    scan_start = max(0, est_start - buffer)
    scan_end = est_end + buffer

    print(f"  UTC day: {day_start_dt.isoformat()} -> {day_end_dt.isoformat()}")
    print(f"  Block range: {scan_start} to {scan_end} ({scan_end - scan_start} blocks, buffer={buffer})")

    return scan_start, scan_end, day_start_ts, day_end_ts
