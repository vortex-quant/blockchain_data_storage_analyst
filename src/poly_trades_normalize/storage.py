"""Parquet I/O — schemas, file path helpers, read/write operations."""

from __future__ import annotations

from pathlib import Path

import polars as pl


# ── Input file naming ─────────────────────────────────────────────────────────
# Raw blockchain files use underscores: polymarket_orders_YYYY_MM_DD.parquet
# Raw events files use underscores:     polymarket_events_YYYY_MM_DD.parquet

def input_orders_path(orders_dir: Path, date_str: str) -> Path:
    """Build input path for a day's order fills.

    date_str: YYYY-MM-DD format
    """
    underscored = date_str.replace("-", "_")
    return orders_dir / f"polymarket_orders_{underscored}.parquet"


def input_events_path(events_dir: Path, date_str: str) -> Path:
    """Build input path for a day's blockchain events.

    date_str: YYYY-MM-DD format
    """
    underscored = date_str.replace("-", "_")
    return events_dir / f"polymarket_events_{underscored}.parquet"


# ── Output file naming ────────────────────────────────────────────────────────
# Normalized output files use hyphens: YYYY-MM-DD.parquet

def output_trades_path(trades_dir: Path, date_str: str) -> Path:
    """Build output path for a day's normalized trades."""
    return trades_dir / f"{date_str}.parquet"


def output_events_path(events_dir: Path, date_str: str) -> Path:
    """Build output path for a day's normalized events."""
    return events_dir / f"{date_str}.parquet"


# ── Column lists ──────────────────────────────────────────────────────────────

# Columns cast to Categorical in the output for memory efficiency.
# These have low cardinality relative to row count.
TRADES_CATEGORICAL_COLS = [
    "asset",
    "event_slug",
    "event_type",
    "market_id",
    "outcome",
    "resolution",
    "side",
    "maker",
]

EVENTS_CATEGORICAL_COLS = [
    "asset",
    "event_slug",
    "event_type",
    "condition_id",
    "resolution",
]


# ── Read / Write ──────────────────────────────────────────────────────────────

def read_parquet(path: Path) -> pl.DataFrame:
    """Read a parquet file into a polars DataFrame."""
    return pl.read_parquet(path)


def write_parquet(
    df: pl.DataFrame,
    final_path: Path,
    compression: str = "zstd",
    compression_level: int = 6,
) -> None:
    """Write a DataFrame to parquet with atomic file replacement.

    Writes to a temp file first, then renames to the final path.
    """
    final_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = final_path.parent / f".tmp_{final_path.name}"

    if temp_path.exists():
        temp_path.unlink()

    try:
        df.write_parquet(
            temp_path,
            compression=compression,
            compression_level=compression_level,
        )
        temp_path.rename(final_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise
