"""Polymarket SQD Portal data fetcher — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
GAMMA_BASE = "https://gamma-api.polymarket.com"
POLYGON_RPC_URL = "https://polygon-rpc.com"

# ── Contract constants ────────────────────────────────────────────────────────

EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
EXCHANGE_V2_LOWER = EXCHANGE_V2.lower()
ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

# ── Event slug patterns ───────────────────────────────────────────────────────

SLUG_PREFIXES = [
    "btc-updown-5m-",
    "eth-updown-5m-",
    "sol-updown-5m-",
    "xrp-updown-5m-",
    "bnb-updown-5m-",
    "doge-updown-5m-",
]

# ── Timing constants ──────────────────────────────────────────────────────────

SLUG_BATCH_SIZE = 20
GAMMA_DELAY = 0.3
SQD_DELAY = 0.55  # 20 req / 10 sec → 0.5s min, 0.55s for safety
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0

POLYGON_BLOCK_TIME = 2.0  # seconds per block (Polygon ~2s)
BLOCK_BUFFER = 5000  # extra blocks around estimated UTC day boundaries

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
WRITE_BATCH_SIZE = 50_000  # rows per batch for streaming parquet writes

# ── SQD Portal ────────────────────────────────────────────────────────────────

SQD_MAX_BLOCKS_PER_REQUEST = 10_000
