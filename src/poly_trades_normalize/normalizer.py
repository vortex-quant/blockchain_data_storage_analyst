"""Core normalization logic — transform raw blockchain data into clean trades and events.

Pipeline (per day):
  1. Load polymarket_events_YYYY_MM_DD.parquet → normalize events
  2. Build token_id → event metadata lookup from events
  3. Load polymarket_orders_YYYY_MM_DD.parquet → filter is_taker=True
  4. Join orders with event lookup → enrich + normalize trades

Key design decisions:
  - Only is_taker=True rows are kept (one row per trade, eliminates double counting)
  - token_id_up / token_id_down are unpivoted to map each token_id to its outcome
  - outcome_prices (["1","0"] or ["0","1"]) is parsed into a resolution column
  - Low-cardinality string columns are cast to Categorical for memory efficiency
"""

from __future__ import annotations

import polars as pl

from poly_trades_normalize.storage import (
    EVENTS_CATEGORICAL_COLS,
    TRADES_CATEGORICAL_COLS,
    input_events_path,
    input_orders_path,
    output_events_path,
    output_trades_path,
    read_parquet,
    write_parquet,
)

# ── Resolution parsing ────────────────────────────────────────────────────────


def _parse_resolution(outcome_prices: str) -> str | None:
    """Parse outcome_prices JSON string into resolution label.

    outcome_prices format: '["1","0"]' → Up won, '["0","1"]' → Down won.
    Returns None for unrecognized formats.
    """
    if outcome_prices == '["1","0"]':
        return "Up"
    if outcome_prices == '["0","1"]':
        return "Down"
    return None


# ── Events normalization ──────────────────────────────────────────────────────


def normalize_events(events_raw: pl.DataFrame) -> pl.DataFrame:
    """Normalize raw blockchain events into the target events schema.

    Input columns (from polymarket_events_YYYY_MM_DD.parquet):
        ts, asset, event_slug, event_type, start_epoch, end_epoch,
        condition_id, token_id_up, token_id_down, question,
        event_id, market_id, outcomes, outcome_prices, question_id,
        neg_risk, volume

    Output columns:
        ts, asset, event_slug, event_type, condition_id,
        token_id_up, token_id_down, start_epoch, end_epoch,
        question, resolution
    """
    return (
        events_raw.with_columns(
            pl.col("outcome_prices")
            .map_elements(_parse_resolution, return_dtype=pl.String)
            .alias("resolution")
        )
        .select(
            pl.col("ts"),
            pl.col("asset"),
            pl.col("event_slug"),
            pl.col("event_type"),
            pl.col("condition_id"),
            pl.col("token_id_up"),
            pl.col("token_id_down"),
            pl.col("start_epoch"),
            pl.col("end_epoch"),
            pl.col("question"),
            pl.col("resolution"),
        )
        .with_columns([pl.col(c).cast(pl.Categorical) for c in EVENTS_CATEGORICAL_COLS])
    )


# ── Token lookup ──────────────────────────────────────────────────────────────


def build_token_lookup(events_raw: pl.DataFrame) -> pl.DataFrame:
    """Build a token_id → event metadata lookup by unpivoting token_id_up/down.

    Each event has two outcome tokens (Up and Down). This creates a long-format
    table with one row per token_id, carrying the event metadata and the
    outcome label/index for that specific token.

    Used as the join key for orders (where token_id identifies which outcome
    token was traded).
    """
    up = events_raw.select(
        pl.col("token_id_up").alias("token_id"),
        pl.col("condition_id"),
        pl.col("event_slug"),
        pl.col("asset"),
        pl.col("event_type"),
        pl.col("start_epoch"),
        pl.col("end_epoch"),
        pl.col("outcome_prices"),
        pl.lit("Up").alias("outcome"),
        pl.lit(0).alias("outcome_index"),
    )

    down = events_raw.select(
        pl.col("token_id_down").alias("token_id"),
        pl.col("condition_id"),
        pl.col("event_slug"),
        pl.col("asset"),
        pl.col("event_type"),
        pl.col("start_epoch"),
        pl.col("end_epoch"),
        pl.col("outcome_prices"),
        pl.lit("Down").alias("outcome"),
        pl.lit(1).alias("outcome_index"),
    )

    return (
        pl.concat([up, down])
        .with_columns(
            pl.col("outcome_prices")
            .map_elements(_parse_resolution, return_dtype=pl.String)
            .alias("resolution")
        )
        .drop("outcome_prices")
    )


# ── Trades normalization ──────────────────────────────────────────────────────


def normalize_trades(
    orders: pl.DataFrame,
    token_lookup: pl.DataFrame,
) -> pl.DataFrame:
    """Normalize raw blockchain order fills into the target trades schema.

    Steps:
      1. Filter to is_taker=True only (one row per trade, no double counting)
      2. Inner join with token_lookup on token_id (crypto events only + enrichment)
      3. Transform columns to match target schema
      4. Cast low-cardinality strings to Categorical

    Input: raw order fills with columns:
        block_number, timestamp, block_time, tx_hash, log_index, order_hash,
        maker, taker, is_taker, is_sell, token_id, fee, amount_usd, shares, price

    Output: normalized trades with columns:
        ts, time, asset, event_slug, event_type, asset_id, market_id,
        outcome, outcome_index, resolution, price, size, side,
        amount_usd, fee, maker
    """
    return (
        orders
        # Step 1: Keep only taker rows — one per trade, eliminates double counting
        .filter(pl.col("is_taker") == True)  # noqa: E712
        # Step 2: Join with event metadata on token_id (inner = crypto events only)
        .join(token_lookup, on="token_id", how="inner")
        # Step 3: Transform to target schema
        .with_columns(
            (pl.col("timestamp") * 1000).alias("time"),
            pl.from_epoch("timestamp", time_unit="s")
            .dt.replace_time_zone("UTC")
            .alias("ts"),
            pl.when(pl.col("is_sell"))
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("BUY"))
            .alias("side"),
            pl.col("token_id").alias("asset_id"),
            pl.col("condition_id").alias("market_id"),
            pl.col("shares").alias("size"),
        )
        .select(
            pl.col("ts"),
            pl.col("time"),
            pl.col("asset"),
            pl.col("event_slug"),
            pl.col("event_type"),
            pl.col("asset_id"),
            pl.col("market_id"),
            pl.col("outcome"),
            pl.col("outcome_index"),
            pl.col("resolution"),
            pl.col("price"),
            pl.col("size"),
            pl.col("side"),
            pl.col("amount_usd"),
            pl.col("fee"),
            pl.col("maker"),
        )
        # Step 4: Cast low-cardinality columns to Categorical
        .with_columns([pl.col(c).cast(pl.Categorical) for c in TRADES_CATEGORICAL_COLS])
        .sort("time")
    )


# ── Day processing ────────────────────────────────────────────────────────────


def process_day(date_str: str) -> tuple[bool, str]:
    """Process a single day: produce both normalized trades and events parquet.

    Returns (success, message). Reads from config constants for all paths and settings.
    Skips if both output files already exist and replace=False.
    """
    trades_out = output_trades_path(config.TRADES_OUTPUT_DIR, date_str)
    events_out = output_events_path(config.EVENTS_OUTPUT_DIR, date_str)

    if not config.REPLACE and trades_out.exists() and events_out.exists():
        return True, f"{date_str}: skipped (already exists)"

    orders_path = input_orders_path(config.ORDERS_DIR, date_str)
    events_path = input_events_path(config.EVENTS_DIR, date_str)

    if not events_path.exists():
        return False, f"{date_str}: events input not found — {events_path}"
    if not orders_path.exists():
        return False, f"{date_str}: orders input not found — {orders_path}"

    events_raw = read_parquet(events_path)
    events_normalized = normalize_events(events_raw)
    write_parquet(
        events_normalized,
        events_out,
        compression=config.PARQUET_COMPRESSION,
        compression_level=config.PARQUET_COMPRESSION_LEVEL,
    )

    token_lookup = build_token_lookup(events_raw)

    orders = read_parquet(orders_path)
    trades = normalize_trades(orders, token_lookup)
    write_parquet(
        trades,
        trades_out,
        compression=config.PARQUET_COMPRESSION,
        compression_level=config.PARQUET_COMPRESSION_LEVEL,
    )

    return True, (
        f"{date_str}: {len(trades):,} trades, {len(events_normalized):,} events"
    )
