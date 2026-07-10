"""Polymarket SQD Portal data fetcher — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
GAMMA_BASE = "https://gamma-api.polymarket.com"

# ── Contract constants ────────────────────────────────────────────────────────

EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
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
SQD_DELAY = 0.1
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0

POLYGON_BLOCK_TIME = 1.5  # seconds per block
BLOCK_BUFFER = 200  # extra blocks before/after estimated range
TIME_BUFFER_SECONDS = 600  # 10-min buffer around event times

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 18

# ── SQD Portal rate limit: 20 requests per 10 seconds ─────────────────────────
SQD_RATE_LIMIT_WINDOW = 10.0  # seconds
SQD_RATE_LIMIT_MAX_REQUESTS = 20
