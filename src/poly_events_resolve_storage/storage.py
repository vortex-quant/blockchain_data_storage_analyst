"""Parquet storage for polymarket event resolutions — minimal schema."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from poly_events_resolve_storage.constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)

# Minimal schema — join with poly_events_storage (App B) on question_id
# for ancillary_data, asset, event_type.
#
# Dropped as redundant: block_time (from timestamp), is_up (from settled_price),
# payouts (from settled_price for binary), adapter_address (not analytical),
# ancillary_data/asset/event_type (available in App B).

RESOLVE_SCHEMA = pa.schema(
    [
        pa.field("block_number", pa.int64()),
        pa.field("timestamp", pa.int64()),
        pa.field("tx_hash", pa.string()),
        pa.field("log_index", pa.int32()),
        pa.field("question_id", pa.string()),
        pa.field("settled_price", pa.int64()),
    ]
)


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
