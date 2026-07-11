# Poly Data Storage

Polymarket data ingestion pipeline — two independent apps that fetch and store
trade and event data as daily parquet files.

## Apps

### poly-fetch (App A) — Order Fills

Fetches raw `OrderFilled` V2 events from the Polygon blockchain via the SQD
Portal. Streams logs block-by-block, decodes maker/taker fills, filters to
exact UTC day boundaries, deduplicates by `(tx_hash, log_index)`, and writes
incrementally to parquet.

**Output**: `polymarket_orders_YYYY_MM_DD.parquet`

### poly-events (App B) — Event Metadata

Fetches all crypto event metadata from the Polymarket Gamma API. Paginates
closed crypto events (including all sub-tags: BTC, ETH, SOL, etc.), filters
client-side by `endDate` to the target UTC day, and stores the full event
structure including nested `markets` (with `conditionId`, `clobTokenIds`,
`questionID`) and `tags` as JSON strings for future joining with App A.

**Output**: `polymarket_events_YYYY_MM_DD.parquet`

## Usage

Both apps accept `--start`, `--end`, `--output`, and `--replace` flags:

```bash
# Fetch order fills for a single day
uv run poly-fetch 2026-07-09

# Fetch order fills for a date range
uv run poly-fetch --start 2026-07-01 --end 2026-07-31

# Fetch events for a date range to a custom directory
uv run poly-events --start 2026-07-10 --end 2026-07-11 --output /mnt/volume1/vol_poly
uv run poly-fetch --start 2026-07-11 --end 2026-07-11 --output ./test/

/home/ubuntu/projects/project_poly

/mnt/volume1/vol_poly

# Overwrite existing files
uv run poly-events --start 2026-07-09 --end 2026-07-09 --replace
```

Can also be run as a module:

```bash
uv run python -m poly_data_storage 2026-07-09
uv run python -m poly_events_storage --start 2026-07-01 --end 2026-07-31
```

## Setup

```bash
uv sync
```

Requires Python >=3.14.5.
