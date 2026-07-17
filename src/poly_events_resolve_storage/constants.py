"""Polymarket event resolution ingestion — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
SQD_TIMESTAMP_URL = (
    "https://portal.sqd.dev/datasets/polygon-mainnet/timestamps/{ts}/block"
)

# ── ConditionalTokens contract on Polygon ────────────────────────────────────

# The Gnosis ConditionalTokens contract emits ConditionResolution for ALL market
# resolutions — both standard (UmaCtfAdapter) and NegRisk markets.
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# ── Event topic0 hashes (keccak256 of signatures) ─────────────────────────────

# ConditionResolution(bytes32 indexed conditionId, address indexed oracle,
#   bytes32 indexed questionId, uint256 outcomeSlotCount, uint256[] payoutNumerators)
CONDITION_RESOLUTION_TOPIC = (
    "0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894"
)

# ── Timing ────────────────────────────────────────────────────────────────────

SQD_DELAY = 0.55
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0
SQD_MAX_BLOCKS_PER_REQUEST = 10_000

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(
    "/Volumes/T9/project_polymarket_database/poly_orders_blockchain_db/orders_data"
)
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
REPLACE = False  # overwrite existing files
