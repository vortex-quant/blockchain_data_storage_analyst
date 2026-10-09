# Blockchain Data Storage Analyst

Blockchain data ingestion pipeline — five independent CLI apps that fetch,
store, and normalize on-chain data as daily parquet files.

Currently supports **Polymarket** (Polygon) and **Chainlink** price feeds.
Designed to be extensible to additional providers (Hyperliquid, etc.).

## Apps

### poly-fetch — Order Fills

Fetches raw `OrderFilled` V2 events from the Polygon blockchain via the SQD
Portal. Streams logs block-by-block, decodes maker/taker fills, filters to
exact UTC day boundaries, deduplicates by `(tx_hash, log_index)`, and writes
incrementally to parquet.

**Output**: `polymarket_orders_YYYY_MM_DD.parquet`

### poly-events — Event Metadata

Fetches all crypto event metadata from the Polymarket Gamma API. Paginates
closed crypto events (including all sub-tags: BTC, ETH, SOL, etc.), filters
client-side by `closedTime` to the target UTC day, and stores normalized
one-row-per-market data with `conditionId`, `clobTokenIds`, `questionID`,
and `tags`.

**Output**: `polymarket_events_YYYY_MM_DD.parquet`

### poly-events-resolve — Event Resolutions

Fetches `ConditionResolution` events from the Gnosis ConditionalTokens contract
on Polygon via SQD Portal. Decodes payout numerators to determine the
settled outcome (1=Up, 0=Down) for each resolved market.

**Output**: `polymarket_events_resolve_YYYY_MM_DD.parquet`

### chainlink-fetch — Asset Prices

Fetches `AnswerUpdated` events from Chainlink Data Feed aggregators on
Polygon via SQD Portal. Resolves proxy → aggregator addresses via storage
reads, decodes price updates for BTC, ETH, SOL, XRP, DOGE, BNB.

**Output**: `chainlink_asset_prices_YYYY_MM_DD.parquet`

### poly-trades-normalize — Trades Normalization

Normalizes raw blockchain orders + events into clean trades and events
parquet files. Reads only from existing parquet files (no API calls).
Filters to `is_taker=True` rows, joins with event metadata by `token_id`,
and adds resolution from `outcome_prices`. Processes days in parallel.

**Output**: `YYYY-MM-DD.parquet` (trades + events, separate directories)

## Usage

All apps accept only `--start` and `--end` (YYYY-MM-DD, inclusive).
All paths and settings are configured in each package's `constants.py` / `config.py`.

```bash
# Fetch order fills for a date range
uv run poly-fetch --start 2026-10-07 --end 2026-10-08

# Fetch events for a single day
uv run poly-events --start 2026-07-09 --end 2026-07-09

# Fetch event resolutions
uv run poly-events-resolve --start 2026-07-01 --end 2026-07-31

# Fetch Chainlink prices
uv run chainlink-fetch --start 2026-07-01 --end 2026-07-31

# Normalize trades from existing parquet files
uv run poly-trades-normalize --start 2026-07-01 --end 2026-07-31
```

Can also be run as a module:

```bash
uv run python -m poly_data_storage --start 2026-07-01 --end 2026-07-31
uv run python -m poly_events_storage --start 2026-07-01 --end 2026-07-31
uv run python -m poly_events_resolve_storage --start 2026-07-01 --end 2026-07-31
uv run python -m chainlink_asset_storage --start 2026-07-01 --end 2026-07-31
uv run python -m poly_trades_normalize --start 2026-07-01 --end 2026-07-31
```

## Setup

```bash
uv sync
```

Requires Python >=3.14.5.

## Configuration

Each app has its own `constants.py` (or `config.py`) in its package directory
under `src/`. Edit the `OUTPUT_DIR`, `REPLACE`, and other constants there.
No CLI flags for paths or settings — only `--start` and `--end`.

## Project Structure

```
src/
├── poly_data_storage/          # poly-fetch — OrderFilled V2 events
├── poly_events_storage/        # poly-events — Gamma API event metadata
├── poly_events_resolve_storage/# poly-events-resolve — UMA resolutions
├── chainlink_asset_storage/    # chainlink-fetch — Chainlink price feeds
└── poly_trades_normalize/      # poly-trades-normalize — trades normalization
```
