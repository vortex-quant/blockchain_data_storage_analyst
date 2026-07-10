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

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
