"""Chainlink asset price storage — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
SQD_TIMESTAMP_URL = (
    "https://portal.sqd.dev/datasets/polygon-mainnet/timestamps/{ts}/block"
)
RPC_URL = "https://polygon-bor-rpc.publicnode.com"

# ── Chainlink Data Feeds on Polygon ───────────────────────────────────────────

# AnswerUpdated(int256,uint256,uint256) topic0 — verified on-chain
ANSWER_UPDATED_TOPIC = (
    "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
)

# Proxy contract addresses (EACAggregatorProxy) on Polygon mainnet
PROXY_ADDRESSES: dict[str, str] = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
    "XRP": "0x785ba89291f676b5386652eB12b30cF361020694",
    "DOGE": "0xbaf9327b6564454F4a3364C33eFeEf032b4b4444",
    "BNB": "0x82a6c4AF830caa6c97bb504425f6A66165C2c26e",
}

# All USD price feeds use 8 decimals on Chainlink Data Feeds
FEED_DECIMALS = 8

# Heartbeat in seconds (used to compute expires_at = updatedAt + heartbeat)
HEARTBEAT_SECONDS = 3600

# Storage slot for currentPhase struct in EACAggregatorProxy
# Phase { uint16 id; address aggregator; } packed in slot 2
# id at bytes 30-31, aggregator at bytes 10-29
PHASE_STORAGE_SLOT = 2

# ── Timing constants ──────────────────────────────────────────────────────────

SQD_DELAY = 0.55
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
WRITE_BATCH_SIZE = 50_000

# ── SQD Portal ────────────────────────────────────────────────────────────────

SQD_MAX_BLOCKS_PER_REQUEST = 10_000
SQD_MIN_BLOCKS_PER_REQUEST = 500
