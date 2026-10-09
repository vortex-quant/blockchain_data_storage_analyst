"""Parquet storage for polymarket_orders — streaming writes via pyarrow."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from poly_data_storage.blockchain_decoder import OrderFill
from poly_data_storage.constants import (
    COLLECTION_VERSION,
    EXCHANGES,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)

COMPLETION_KEY = "poly_data_storage.completion"

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


def write_order_fills_batch(writer: pq.ParquetWriter, batch: list[OrderFill]) -> None:
    """Write a batch of order fill dicts to the parquet writer."""
    table = pa.Table.from_pylist(batch, schema=ORDER_FILLS_SCHEMA)
    writer.write_table(table)


def finish_order_fills(writer: pq.ParquetWriter, metadata: dict[str, Any]) -> None:
    """Mark a fully reconciled scan in the footer without changing any columns."""
    expected_blocks = metadata["end_block"] - metadata["start_block"] + 1
    if (
        metadata["blocks"] != expected_blocks
        or metadata["matching_logs"] != metadata["rows"]
        or metadata["decoded_rows"] != metadata["rows"]
        or metadata["next_day_block"] > metadata["finalized_head"]
    ):
        raise ValueError("Daily scan counts or finalized boundaries do not reconcile")
    metadata = dict(
        metadata,
        complete=True,
        collection_version=COLLECTION_VERSION,
        exchanges=EXCHANGES,
    )
    writer.add_key_value_metadata({COMPLETION_KEY: orjson.dumps(metadata)})


def is_complete_file(path: Path, date_str: str) -> bool:
    """Only skip files produced by this complete collection/decoder revision."""
    try:
        footer = pq.read_metadata(path)
        if not footer.schema.to_arrow_schema().equals(
            ORDER_FILLS_SCHEMA, check_metadata=False
        ):
            return False
        metadata = orjson.loads((footer.metadata or {})[COMPLETION_KEY.encode()])
        return (
            metadata["complete"] is True
            and metadata["collection_version"] == COLLECTION_VERSION
            and metadata["exchanges"] == EXCHANGES
            and metadata["date"] == date_str
            and metadata["rows"] == footer.num_rows
            and metadata["matching_logs"] == metadata["decoded_rows"] == footer.num_rows
            and metadata["blocks"]
            == metadata["end_block"] - metadata["start_block"] + 1
            and metadata["next_day_block"] == metadata["end_block"] + 1
            and metadata["next_day_block"] <= metadata["finalized_head"]
        )
    except OSError, ValueError, KeyError, TypeError, pa.ArrowException:
        return False
