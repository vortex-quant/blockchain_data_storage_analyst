"""Configuration — all paths and processing settings as constants."""

from __future__ import annotations

from pathlib import Path

# ── Input directories (where raw blockchain parquet files live) ───────────────

ORDERS_DIR = Path("data")
EVENTS_DIR = Path("data")

# ── Output directories (where normalized parquet files are written) ───────────

TRADES_OUTPUT_DIR = Path("data/trades")
EVENTS_OUTPUT_DIR = Path("data/events")

# ── Processing settings ───────────────────────────────────────────────────────

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
MAX_WORKERS = 4
REPLACE = False
