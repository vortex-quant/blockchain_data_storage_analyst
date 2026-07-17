"""Parquet storage for polymarket_events — normalized one-row-per-market schema."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from poly_events_storage.constants import (
    ASSET_TAG_MAP,
    EVENT_TYPE_DURATION_SEC,
    EVENT_TYPE_TAG_MAP,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)

# ── Schema ────────────────────────────────────────────────────────────────────
# Core columns match the target format in /Volumes/T9/project_polymarket_database/events.
# Additional columns retain useful event/market metadata without nesting.

EVENTS_SCHEMA = pa.schema(
    [
        # Core normalized columns (target schema)
        pa.field("ts", pa.timestamp("us", tz="UTC")),
        pa.field("asset", pa.string()),
        pa.field("event_slug", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("start_epoch", pa.int64()),
        pa.field("end_epoch", pa.int64()),
        pa.field("condition_id", pa.string()),
        pa.field("token_id_up", pa.string()),
        pa.field("token_id_down", pa.string()),
        pa.field("question", pa.string()),
        # Additional useful columns
        pa.field("event_id", pa.int64()),
        pa.field("market_id", pa.int64()),
        pa.field("outcomes", pa.string()),
        pa.field("outcome_prices", pa.string()),
        pa.field("question_id", pa.string()),
        pa.field("neg_risk", pa.bool_()),
        pa.field("volume", pa.float64()),
    ]
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _to_int(value, default=0):
    if value is None:
        return default
    try:
        return int(value)
    except TypeError, ValueError:
        return default


def _to_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except TypeError, ValueError:
        return default


def _parse_iso(date_str: str | None) -> datetime | None:
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_json_str(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = orjson.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _parse_asset(tags: list[dict]) -> str | None:
    for tag in tags:
        slug = tag.get("slug", "")
        if slug in ASSET_TAG_MAP:
            return ASSET_TAG_MAP[slug]
    return None


def _parse_event_type(tags: list[dict]) -> str | None:
    for tag in tags:
        slug = tag.get("slug", "")
        if slug in EVENT_TYPE_TAG_MAP:
            return EVENT_TYPE_TAG_MAP[slug]
    return None


# ── Normalization ─────────────────────────────────────────────────────────────


def normalize_event(event: dict) -> list[dict]:
    """Normalize a raw Gamma API event into one row per market.

    Extracts asset and event_type from tags, condition_id and token IDs
    from each market, and calculates start_epoch/end_epoch.
    """
    tags = event.get("tags") or []
    if isinstance(tags, str):
        tags = _parse_json_str(tags)

    asset = _parse_asset(tags)
    event_type = _parse_event_type(tags)
    event_slug = event.get("slug")
    closed_dt = _parse_iso(event.get("closedTime"))
    ts = closed_dt if closed_dt else None

    event_id = _to_int(event.get("id"))
    neg_risk = bool(event.get("negRisk", False))
    volume = _to_float(event.get("volume"))

    markets = event.get("markets") or []
    if isinstance(markets, str):
        markets = _parse_json_str(markets)

    rows: list[dict] = []
    for market in markets:
        condition_id = market.get("conditionId")
        question = market.get("question")
        market_id = _to_int(market.get("id"))

        clob_ids = _parse_json_str(market.get("clobTokenIds"))
        token_id_up = clob_ids[0] if len(clob_ids) > 0 else None
        token_id_down = clob_ids[1] if len(clob_ids) > 1 else None

        outcomes = _parse_json_str(market.get("outcomes"))
        outcome_prices = _parse_json_str(market.get("outcomePrices"))

        # Calculate end_epoch from market endDate
        end_dt = _parse_iso(market.get("endDate"))
        end_epoch = int(end_dt.timestamp()) if end_dt else None

        # Calculate start_epoch
        start_epoch = None
        if (
            end_epoch is not None
            and event_type
            and event_type in EVENT_TYPE_DURATION_SEC
        ):
            start_epoch = end_epoch - EVENT_TYPE_DURATION_SEC[event_type]
        else:
            start_dt = _parse_iso(market.get("startDate"))
            if start_dt:
                start_epoch = int(start_dt.timestamp())

        rows.append(
            {
                "ts": ts,
                "asset": asset,
                "event_slug": event_slug,
                "event_type": event_type,
                "start_epoch": start_epoch if start_epoch is not None else 0,
                "end_epoch": end_epoch if end_epoch is not None else 0,
                "condition_id": condition_id if condition_id else "",
                "token_id_up": token_id_up if token_id_up else "",
                "token_id_down": token_id_down if token_id_down else "",
                "question": question if question else "",
                "event_id": event_id,
                "market_id": market_id,
                "outcomes": orjson.dumps(outcomes).decode() if outcomes else "[]",
                "outcome_prices": orjson.dumps(outcome_prices).decode()
                if outcome_prices
                else "[]",
                "question_id": market.get("questionID") or "",
                "neg_risk": neg_risk,
                "volume": volume,
            }
        )

    return rows


# ── Storage ───────────────────────────────────────────────────────────────────


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


def write_events_batch(writer: pq.ParquetWriter, batch: list[dict]) -> int:
    """Write a batch of raw event dicts, normalized to one row per market.

    Returns the number of normalized rows written.
    """
    rows: list[dict] = []
    for event in batch:
        rows.extend(normalize_event(event))
    if rows:
        table = pa.Table.from_pylist(rows, schema=EVENTS_SCHEMA)
        writer.write_table(table)
    return len(rows)
