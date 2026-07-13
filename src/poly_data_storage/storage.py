"""Parquet storage for polymarket_orders — streaming writes via pyarrow."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from poly_data_storage.constants import PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL

ORDER_FILLS_SCHEMA = pa.schema(
    [
        pa.field("block_number", pa.int64()),
        pa.field("timestamp", pa.int64()),
        pa.field("block_time", pa.string()),
        pa.field("tx_hash", pa.string()),
        pa.field("log_index", pa.int32()),
        pa.field("order_hash", pa.string()),
        pa.field("maker", pa.string()),
        pa.field("taker", pa.string()),
        pa.field("is_taker", pa.bool_()),
        pa.field("is_sell", pa.bool_()),
        pa.field("token_id", pa.string()),
        pa.field("fee", pa.float64()),
        pa.field("amount_usd", pa.float64()),
        pa.field("shares", pa.float64()),
        pa.field("price", pa.float64()),
    ]
)


def ensure_output_dir(output_dir: Path) -> Path:
    """Create and return the output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def open_order_fills_writer(path: Path) -> pq.ParquetWriter:
    """Open a ParquetWriter for incremental order fills writes."""
    return pq.ParquetWriter(
        str(path),
        ORDER_FILLS_SCHEMA,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )


def write_order_fills_batch(writer: pq.ParquetWriter, batch: list[dict]) -> None:
    """Write a batch of order fill dicts to the parquet writer."""
    table = pa.Table.from_pylist(batch, schema=ORDER_FILLS_SCHEMA)
    writer.write_table(table)
