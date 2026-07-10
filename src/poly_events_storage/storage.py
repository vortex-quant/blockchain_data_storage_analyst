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
    pa.field("title", pa.string()),
    pa.field("description", pa.string()),
    pa.field("category", pa.string()),
    pa.field("startDate", pa.string()),
    pa.field("endDate", pa.string()),
    pa.field("closedTime", pa.string()),
    pa.field("active", pa.bool_()),
    pa.field("closed", pa.bool_()),
    pa.field("archived", pa.bool_()),
    pa.field("volume", pa.float64()),
    pa.field("volume24hr", pa.float64()),
    pa.field("volumeNum", pa.float64()),
    pa.field("liquidity", pa.float64()),
    pa.field("liquidityNum", pa.float64()),
    pa.field("tags", pa.string()),
    pa.field("markets", pa.string()),
    pa.field("createdAt", pa.string()),
    pa.field("updatedAt", pa.string()),
    pa.field("image", pa.string()),
    pa.field("icon", pa.string()),
])


def _serialize(value) -> str:
    """Serialize a value to a JSON string, handling already-string inputs."""
    if value is None:
        return "[]"
    if isinstance(value, str):
        return value
    return orjson.dumps(value).decode()


def flatten_event(event: dict) -> dict:
    """Flatten a raw Gamma API event dict to match EVENTS_SCHEMA."""
    return {
        "id": event.get("id"),
        "slug": event.get("slug"),
        "title": event.get("title"),
        "description": event.get("description"),
        "category": event.get("category"),
        "startDate": event.get("startDate"),
        "endDate": event.get("endDate"),
        "closedTime": event.get("closedTime"),
        "active": event.get("active"),
        "closed": event.get("closed"),
        "archived": event.get("archived"),
        "volume": event.get("volume"),
        "volume24hr": event.get("volume24hr"),
        "volumeNum": event.get("volumeNum"),
        "liquidity": event.get("liquidity"),
        "liquidityNum": event.get("liquidityNum"),
        "tags": _serialize(event.get("tags")),
        "markets": _serialize(event.get("markets")),
        "createdAt": event.get("createdAt"),
        "updatedAt": event.get("updatedAt"),
        "image": event.get("image"),
        "icon": event.get("icon"),
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
