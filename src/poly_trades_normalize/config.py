"""Configuration — all paths and processing settings as constants."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# ── Input directories (where raw blockchain parquet files live) ───────────────

# Directory containing polymarket_orders_YYYY_MM_DD.parquet files
ORDERS_DIR = Path(
    "/Volumes/T9/project_polymarket_database/poly_orders_blockchain_db/orders_data"
)

# Directory containing polymarket_events_YYYY_MM_DD.parquet files
EVENTS_DIR = Path(
    "/Volumes/T9/project_polymarket_database/poly_orders_blockchain_db/orders_data"
)

# ── Output directories (where normalized parquet files are written) ───────────

# Directory for normalized trades output: YYYY-MM-DD.parquet
TRADES_OUTPUT_DIR = Path("/Volumes/T9/project_polymarket_database/poly_trades")

# Directory for normalized events output: YYYY-MM-DD.parquet
EVENTS_OUTPUT_DIR = Path("/Volumes/T9/project_polymarket_database/poly_events")

# ── Processing settings ───────────────────────────────────────────────────────

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 16
MAX_WORKERS = 4
REPLACE = True


@dataclass(frozen=True)
class Config:
    """Runtime configuration assembled from module constants."""

    orders_dir: Path
    events_dir: Path
    trades_output_dir: Path
    events_output_dir: Path
    parquet_compression: str
    parquet_compression_level: int
    max_workers: int
    replace: bool


def get_config() -> Config:
    """Build a Config instance from module-level constants."""
    return Config(
        orders_dir=ORDERS_DIR,
        events_dir=EVENTS_DIR,
        trades_output_dir=TRADES_OUTPUT_DIR,
        events_output_dir=EVENTS_OUTPUT_DIR,
        parquet_compression=PARQUET_COMPRESSION,
        parquet_compression_level=PARQUET_COMPRESSION_LEVEL,
        max_workers=MAX_WORKERS,
        replace=REPLACE,
    )
