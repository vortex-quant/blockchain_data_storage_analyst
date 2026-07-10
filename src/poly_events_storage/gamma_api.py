"""Gamma API client — fetch all crypto events for a UTC day.

The Gamma API's ``end_date_min``/``end_date_max`` filters by ``endDate``
(not ``closedTime``), and the API caps at offset 2100.  To fetch all
events for a day we split the day into sub-windows and paginate each,
ordering by ``closedTime`` descending and filtering precisely client-side.
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
    end_date_min: str | None = None,
    end_date_max: str | None = None,
) -> list[dict]:
    """Fetch one page of closed crypto events from Gamma API.

    Optional ``end_date_min`` / ``end_date_max`` narrow the server-side
    result set by ``endDate`` (ISO-8601 format).  We still filter by
    ``closedTime`` client-side because ``endDate`` and ``closedTime``
    can differ by hours.
    """
    params = {
        "tag_slug": GAMMA_TAG_SLUG,
        "related_tags": str(GAMMA_RELATED_TAGS).lower(),
        "closed": "true",
        "limit": GAMMA_PAGE_SIZE,
        "offset": offset,
        "order": "closedTime",
        "ascending": "false",
    }
    if end_date_min:
        params["end_date_min"] = end_date_min
    if end_date_max:
        params["end_date_max"] = end_date_max

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(f"{GAMMA_BASE}/events", params=params, timeout=30.0)
            # 422 at high offsets means we've exhausted the API's offset limit
            if resp.status_code == 422:
                print(f"      Gamma API offset limit reached (422) at offset={offset}")
                return []
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


# Split day into sub-windows to stay under the API's 2100-offset cap.
# Each 12-hour window typically has ~1500 events — well under the limit.
SUB_WINDOW_HOURS = 12


def fetch_crypto_events_for_day(
    client: niquests.Session,
    date_str: str,
) -> tuple[list[dict], list[str]]:
    """Fetch all crypto events whose closedTime falls within a UTC day.

    Splits the day into 12-hour ``endDate`` sub-windows and paginates each
    with ``closedTime`` descending.  Filters precisely client-side by
    ``closedTime``.  Stops each sub-window early once events are older
    than the sub-window's start.

    Returns (events, failures) where failures is a list of error messages.
    """
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    all_events: list[dict] = []
    seen_ids: set[int] = set()
    failures: list[str] = []
    page = 0

    # Build sub-windows covering [day_start, day_end] with a small lower
    # buffer so events whose endDate is slightly before day_start but
    # closedTime is within the day are not missed.
    win_start = day_start - timedelta(hours=1)
    while win_start < day_end:
        win_end = min(win_start + timedelta(hours=SUB_WINDOW_HOURS), day_end + timedelta(hours=1))
        end_date_min = win_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_max = win_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        offset = 0

        while True:
            page += 1
            try:
                data = _fetch_page(client, offset, end_date_min, end_date_max)
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

                closed_dt = _parse_iso(event.get("closedTime"))
                if closed_dt is None:
                    continue

                if closed_dt >= day_end:
                    continue
                if closed_dt < day_start:
                    past_day = True
                    break

                all_events.append(event)
                page_matches += 1

            print(f"  [page {page}] offset={offset}: {page_matches} matches (total: {len(all_events)})")

            if past_day:
                break
            if len(data) < GAMMA_PAGE_SIZE:
                break

            offset += len(data)
            time.sleep(GAMMA_DELAY)

        win_start = win_end

    return all_events, failures
