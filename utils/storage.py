"""Polymarket SQD Portal data fetcher — parquet storage with zstd compression."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.constants import OUTPUT_DIR, PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL


def _ensure_output_dir(output_dir: Path | None = None) -> Path:
    """Create and return the output directory."""
    target_dir = output_dir or OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def _add_datetime_column(df: pl.DataFrame) -> pl.DataFrame:
    """Add a datetime column from the Unix timestamp column if present."""
    if "timestamp" in df.columns:
        return df.with_columns(
            pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="s")
            .cast(pl.Datetime("us"))
            .alias("datetime")
        )
    return df


def save_trades(
    trades: list[dict],
    date_str: str,
    output_dir: Path | None = None,
) -> Path | None:
    """Save decoded trades to a parquet file with zstd compression.

    File name: polymarket_trades_YYYY_MM_DD.parquet
    Returns the path to the saved file, or None if no trades.
    """
    if not trades:
        print("  No trades to save!")
        return None

    out_dir = _ensure_output_dir(output_dir)
    df = _add_datetime_column(pl.DataFrame(trades))

    filename = f"polymarket_trades_{date_str.replace('-', '_')}.parquet"
    filepath = out_dir / filename
    df.write_parquet(
        filepath,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )
    print(f"  Saved {len(trades)} trades to {filepath}")
    return filepath


def save_events(
    events: list[dict],
    date_str: str,
    output_dir: Path | None = None,
) -> Path | None:
    """Save event metadata to a parquet file with zstd compression.

    File name: polymarket_events_YYYY_MM_DD.parquet
    Returns the path to the saved file, or None if no events.
    """
    if not events:
        print("  No events to save!")
        return None

    out_dir = _ensure_output_dir(output_dir)
    df = pl.DataFrame(events)

    filename = f"polymarket_events_{date_str.replace('-', '_')}.parquet"
    filepath = out_dir / filename
    df.write_parquet(
        filepath,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )
    print(f"  Saved {len(events)} events to {filepath}")
    return filepath
