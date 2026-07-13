"""Polymarket order fill ingestion — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
SQD_TIMESTAMP_URL = (
    "https://portal.sqd.dev/datasets/polygon-mainnet/timestamps/{ts}/block"
)

# ── Contract constants ────────────────────────────────────────────────────────

EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
EXCHANGE_V2_LOWER = EXCHANGE_V2.lower()
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)

# ── Timing constants ──────────────────────────────────────────────────────────

SQD_DELAY = 0.55  # 20 req / 10 sec → 0.5s min, 0.55s for safety
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
WRITE_BATCH_SIZE = 50_000  # rows per batch for streaming parquet writes

# ── SQD Portal ────────────────────────────────────────────────────────────────

SQD_MAX_BLOCKS_PER_REQUEST = 10_000
SQD_MIN_BLOCKS_PER_REQUEST = 500  # floor for adaptive splitting on stream resets
