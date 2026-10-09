"""Polymarket order fill ingestion — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

SQD_BASE_URL = "https://portal.sqd.dev/datasets/polygon-mainnet"
SQD_URL = f"{SQD_BASE_URL}/finalized-stream"
SQD_FINALIZED_HEAD_URL = f"{SQD_BASE_URL}/finalized-head"
SQD_TIMESTAMP_URL = (
    "https://portal.sqd.dev/datasets/polygon-mainnet/timestamps/{ts}/block"
)

# ── Contract constants ────────────────────────────────────────────────────────

# Query every supported deployment, including overlapping historical versions.
# Do not use first/last observed fills as deployment or retirement cutoffs.
EXCHANGES = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "v1",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a": "v1",
    "0xe111180000d2663c0091e4f400237545b87b996b": "v2",
    "0xe2222d279d744050d28e00520010520000310f59": "v2",
    "0xe3333700ca9d93003f00f0f71f8515005f6c00aa": "v3",
}
ORDER_FILLED_V1_TOPIC = (
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
)
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)

# ── Timing constants ──────────────────────────────────────────────────────────

SQD_DELAY = 0.55  # conservative pacing between successful stream requests
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data"
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
WRITE_BATCH_SIZE = 50_000  # rows per batch for streaming parquet writes
REPLACE = False  # overwrite existing files

# Bump whenever decoding/collection semantics change. Stored in Parquet metadata.
COLLECTION_VERSION = "2"
