"""Block estimation helpers — SQD Portal timestamp lookup and RPC fallback."""

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
    MAX_RETRIES,
    POLYGON_BLOCK_TIME,
    POLYGON_RPC_FALLBACKS,
    RETRY_BASE_DELAY,
    SQD_TIMESTAMP_URL,
)
from poly_data_storage.logger import get_logger

log = get_logger()


def _load_alchemy_key() -> str | None:
    """Load Alchemy API key from keys.env in the project root."""
    key = os.environ.get(ALCHEMY_API_KEY_ENV)
    if key:
        return key
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


def _sqd_timestamp_to_block(client: niquests.Session, target_ts: int) -> int:
    """Use SQD Portal's timestamp-to-block endpoint for exact block lookup."""
    url = SQD_TIMESTAMP_URL.format(ts=target_ts)
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(url, timeout=30.0)
            if resp.status_code == 404:
                raise ValueError(f"No block found at or after timestamp {target_ts}")
            resp.raise_for_status()
            return resp.json()["block_number"]
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                log.info(f"  SQD timestamp lookup retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                time.sleep(delay)
            else:
                raise


def _rpc_get_block_by_timestamp(
    client: niquests.Session,
    target_ts: int,
    known_block: int,
    known_block_ts: int,
) -> int:
    """Fallback: estimate block via RPC + binary search."""
    delta_seconds = target_ts - known_block_ts
    delta_blocks = int(delta_seconds / POLYGON_BLOCK_TIME)
    estimate = known_block + delta_blocks

    lo = max(0, estimate - BLOCK_BUFFER_BASE)
    hi = estimate + BLOCK_BUFFER_BASE

    for _ in range(30):
        if lo >= hi:
            return lo
        mid = (lo + hi) // 2
        block_hex = hex(mid)
        result = _rpc_call(client, "eth_getBlockByNumber", [block_hex, False])
        block_ts = int(result["timestamp"], 16)
        if block_ts < target_ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


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
                log.info(f"  RPC retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                time.sleep(delay)
            else:
                raise


def estimate_block_range(
    client: niquests.Session,
    date_str: str,
) -> tuple[int, int, int, int]:
    """Estimate block range covering a UTC day.

    Uses SQD Portal's timestamp-to-block endpoint for exact block lookup.
    Falls back to RPC binary search if SQD Portal is unavailable.

    Returns (start_block, end_block, day_start_ts, day_end_ts).
    """
    day_start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end_dt = day_start_dt + timedelta(days=1)
    day_start_ts = int(day_start_dt.timestamp())
    day_end_ts = int(day_end_dt.timestamp())

    # Try SQD Portal timestamp endpoint first (exact, no estimation needed)
    try:
        log.info("  Resolving block range via SQD Portal timestamp endpoint...")
        start_block = _sqd_timestamp_to_block(client, day_start_ts)
        end_block = _sqd_timestamp_to_block(client, day_end_ts)
        log.info(f"  UTC day: {day_start_dt.isoformat()} -> {day_end_dt.isoformat()}")
        log.info(f"  Block range: {start_block} to {end_block} ({end_block - start_block} blocks)")
        return start_block, end_block, day_start_ts, day_end_ts
    except Exception as exc:
        log.info(f"  SQD timestamp endpoint failed ({exc}), falling back to RPC...")

    # Fallback: RPC binary search
    latest_block, latest_ts = get_latest_block_info(client)
    log.info(f"  Latest block: {latest_block} (ts: {datetime.fromtimestamp(latest_ts, tz=UTC)})")

    start_block = _rpc_get_block_by_timestamp(client, day_start_ts, latest_block, latest_ts)
    end_block = _rpc_get_block_by_timestamp(client, day_end_ts, latest_block, latest_ts)

    log.info(f"  UTC day: {day_start_dt.isoformat()} -> {day_end_dt.isoformat()}")
    log.info(f"  Block range: {start_block} to {end_block} ({end_block - start_block} blocks)")

    return start_block, end_block, day_start_ts, day_end_ts
