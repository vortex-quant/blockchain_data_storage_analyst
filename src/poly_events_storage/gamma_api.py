"""Gamma API client — fetch all crypto events for a UTC day.

The Gamma API's date filter is unreliable, so we paginate all closed crypto
events ordered by endDate descending and filter client-side. We stop early
once we pass the target day's start boundary.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import niquests
import orjson

from poly_events_storage.constants import (
    GAMMA_BASE,
    GAMMA_DELAY,
    GAMMA_PAGE_SIZE,
    GAMMA_RELATED_TAGS,
    GAMMA_TAG_SLUG,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
)


def _parse_iso(date_str: str | None) -> datetime | None:
    """Parse an ISO 8601 string to a timezone-aware UTC datetime."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _fetch_page(
    client: niquests.Session,
    offset: int,
) -> list[dict]:
    """Fetch one page of closed crypto events from Gamma API."""
    params = {
        "tag_slug": GAMMA_TAG_SLUG,
        "related_tags": str(GAMMA_RELATED_TAGS).lower(),
        "closed": "true",
        "limit": GAMMA_PAGE_SIZE,
        "offset": offset,
        "order": "endDate",
        "ascending": "false",
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(f"{GAMMA_BASE}/events", params=params, timeout=30.0)
            resp.raise_for_status()
            data = orjson.loads(resp.content)
            return data if isinstance(data, list) else []
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                print(f"      Gamma retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                time.sleep(delay)
            else:
                raise


def fetch_crypto_events_for_day(
    client: niquests.Session,
    date_str: str,
) -> tuple[list[dict], list[str]]:
    """Fetch all crypto events whose endDate falls within a UTC day.

    Paginates through Gamma API closed crypto events ordered by endDate
    descending. Filters client-side because Gamma's date filter is unreliable.
    Stops early once events are older than the target day.

    Returns (events, failures) where failures is a list of error messages.
    """
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    all_events: list[dict] = []
    seen_ids: set[int] = set()
    failures: list[str] = []
    offset = 0
    page = 0

    while True:
        page += 1
        try:
            data = _fetch_page(client, offset)
        except Exception as exc:
            failures.append(f"page_{page}_offset_{offset}: {exc}")
            print(f"  [page {page}] FAILED: {exc}")
            break

        if not data:
            break

        page_matches = 0
        past_day = False

        for event in data:
            event_id = event.get("id")
            if event_id is not None and event_id in seen_ids:
                continue
            if event_id is not None:
                seen_ids.add(event_id)

            end_dt = _parse_iso(event.get("endDate") or event.get("closedTime"))
            if end_dt is None:
                continue

            if end_dt >= day_end:
                continue
            if end_dt < day_start:
                past_day = True
                break

            all_events.append(event)
            page_matches += 1

        print(f"  [page {page}] offset={offset}: {page_matches} matches (total: {len(all_events)})")

        if past_day:
            break
        if len(data) < GAMMA_PAGE_SIZE:
            break

        offset += GAMMA_PAGE_SIZE
        time.sleep(GAMMA_DELAY)

    return all_events, failures
