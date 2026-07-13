"""Parquet storage for polymarket event resolutions — minimal schema."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from poly_events_resolve_storage.constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)

# Non-redundant schema:
# - block_number, timestamp, tx_hash, log_index: blockchain context
# - question_id: unique market identifier
# - settled_price: 1=Up, 0=Down (the outcome)
# - ancillary_data: raw question text (for verification + parsing)
# - asset, event_type: parsed from ancillary_data (pre-computed for filtering)
#
# Dropped as redundant: block_time (from timestamp), is_up (from settled_price),
# payouts (from settled_price for binary), adapter_address (not analytical).

RESOLVE_SCHEMA = pa.schema([
    pa.field("block_number", pa.int64()),
    pa.field("timestamp", pa.int64()),
    pa.field("tx_hash", pa.string()),
    pa.field("log_index", pa.int32()),
    pa.field("question_id", pa.string()),
    pa.field("settled_price", pa.int64()),
    pa.field("ancillary_data", pa.string()),
    pa.field("asset", pa.string()),
    pa.field("event_type", pa.string()),
])


def write_parquet(rows: list[dict], path: Path) -> int:
    """Write rows to a parquet file. Returns row count."""
    table = pa.Table.from_pylist(rows, schema=RESOLVE_SCHEMA)
    pq.write_table(
        table,
        str(path),
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )
    return len(rows)
