"""Parquet storage for polymarket_events — streaming writes via pyarrow."""

from __future__ import annotations

from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from poly_events_storage.constants import PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL

EVENTS_SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("slug", pa.string()),
    pa.field("ticker", pa.string()),
    pa.field("title", pa.string()),
    pa.field("description", pa.string()),
    pa.field("startDate", pa.string()),
    pa.field("creationDate", pa.string()),
    pa.field("endDate", pa.string()),
    pa.field("closedTime", pa.string()),
    pa.field("active", pa.bool_()),
    pa.field("closed", pa.bool_()),
    pa.field("archived", pa.bool_()),
    pa.field("restricted", pa.bool_()),
    pa.field("negRisk", pa.bool_()),
    pa.field("enableNegRisk", pa.bool_()),
    pa.field("volume", pa.float64()),
    pa.field("volume24hr", pa.float64()),
    pa.field("openInterest", pa.float64()),
    pa.field("commentCount", pa.int32()),
    pa.field("tags", pa.string()),
    pa.field("markets", pa.string()),
    pa.field("createdAt", pa.string()),
    pa.field("updatedAt", pa.string()),
])


def _serialize(value) -> str:
    """Serialize a value to a JSON string, handling already-string inputs."""
    if value is None:
        return "[]"
    if isinstance(value, str):
        return value
    return orjson.dumps(value).decode()


def _to_int(value, default=0):
    """Convert a value to int, handling string-encoded integers."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default=0.0):
    """Convert a value to float, handling string-encoded floats."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def flatten_event(event: dict) -> dict:
    """Flatten a raw Gamma API event dict to match EVENTS_SCHEMA.

    The `markets` field is serialized as a JSON string preserving all
    market-level data including conditionId, clobTokenIds, questionID,
    outcomes, outcomePrices — needed for future matching with App A
    order fills by token_id.
    The `tags` field is similarly serialized as a JSON string.
    """
    return {
        "id": _to_int(event.get("id")),
        "slug": event.get("slug"),
        "ticker": event.get("ticker"),
        "title": event.get("title"),
        "description": event.get("description"),
        "startDate": event.get("startDate"),
        "creationDate": event.get("creationDate"),
        "endDate": event.get("endDate"),
        "closedTime": event.get("closedTime"),
        "active": event.get("active"),
        "closed": event.get("closed"),
        "archived": event.get("archived"),
        "restricted": event.get("restricted"),
        "negRisk": event.get("negRisk"),
        "enableNegRisk": event.get("enableNegRisk"),
        "volume": _to_float(event.get("volume")),
        "volume24hr": _to_float(event.get("volume24hr")),
        "openInterest": _to_float(event.get("openInterest")),
        "commentCount": _to_int(event.get("commentCount")),
        "tags": _serialize(event.get("tags")),
        "markets": _serialize(event.get("markets")),
        "createdAt": event.get("createdAt"),
        "updatedAt": event.get("updatedAt"),
    }


def ensure_output_dir(output_dir: Path) -> Path:
    """Create and return the output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def open_events_writer(path: Path) -> pq.ParquetWriter:
    """Open a ParquetWriter for incremental event writes."""
    return pq.ParquetWriter(
        str(path),
        EVENTS_SCHEMA,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )


def write_events_batch(writer: pq.ParquetWriter, batch: list[dict]) -> None:
    """Write a batch of flattened event dicts to the parquet writer."""
    rows = [flatten_event(e) for e in batch]
    table = pa.Table.from_pylist(rows, schema=EVENTS_SCHEMA)
    writer.write_table(table)
