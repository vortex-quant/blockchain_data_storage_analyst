"""Polymarket events ingestion — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

GAMMA_BASE = "https://gamma-api.polymarket.com"

# ── Gamma API parameters ──────────────────────────────────────────────────────

GAMMA_TAG_SLUG = "crypto"
GAMMA_RELATED_TAGS = True
GAMMA_PAGE_SIZE = 100  # API silently caps at 100 per page
GAMMA_DELAY = 0.1  # ~30 req/sec sustained is safe
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0

# ── Normalization mappings ────────────────────────────────────────────────────

# Tag slug → asset symbol (for updown price events)
ASSET_TAG_MAP: dict[str, str] = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "xrp": "XRP",
    "ripple": "XRP",
    "dogecoin": "DOGE",
    "bnb": "BNB",
    "hype": "HYPE",
}

# Tag slug → normalized event type
EVENT_TYPE_TAG_MAP: dict[str, str] = {
    "5M": "5m",
    "15M": "15m",
    "1H": "1h",
    "4h": "4h",
    "daily": "daily",
}

# Event type → duration in seconds (for calculating start_epoch from end_epoch)
EVENT_TYPE_DURATION_SEC: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14_400,
    "daily": 86_400,
}

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
