"""Parquet storage for Chainlink asset prices — streaming writes via pyarrow.

Schema mirrors Chainlink Data Streams v3 (Crypto Advanced) fields where possible.
Bid/ask columns are omitted (not available from Data Feeds).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from chainlink_asset_storage.constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)

ASSET_PRICES_SCHEMA = pa.schema(
    [
        pa.field("block_number", pa.int64()),
        pa.field("timestamp", pa.int64()),
        pa.field("block_time", pa.string()),
        pa.field("tx_hash", pa.string()),
        pa.field("log_index", pa.int32()),
        pa.field("asset", pa.string()),
        pa.field("aggregator_address", pa.string()),
        pa.field("round_id", pa.int64()),
        pa.field("valid_from_timestamp", pa.int64()),
        pa.field("observations_timestamp", pa.int64()),
        pa.field("expires_at", pa.int64()),
        pa.field("price", pa.float64()),
    ]
)


def ensure_output_dir(output_dir: Path) -> Path:
    """Create and return the output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def open_asset_prices_writer(path: Path) -> pq.ParquetWriter:
    """Open a ParquetWriter for incremental asset price writes."""
    return pq.ParquetWriter(
        str(path),
        ASSET_PRICES_SCHEMA,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )


def write_asset_prices_batch(writer: pq.ParquetWriter, batch: list[dict]) -> None:
    """Write a batch of asset price dicts to the parquet writer."""
    table = pa.Table.from_pylist(batch, schema=ASSET_PRICES_SCHEMA)
    writer.write_table(table)
