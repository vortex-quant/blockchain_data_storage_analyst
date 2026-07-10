"""Polymarket SQD Portal data fetcher — Gamma API event metadata."""

from __future__ import annotations

import time
from datetime import UTC, datetime

import niquests
import orjson

from utils.constants import GAMMA_BASE, GAMMA_DELAY, MAX_RETRIES, RETRY_BASE_DELAY, SLUG_BATCH_SIZE


def compute_5min_timestamps(date_str: str) -> list[int]:
    """Compute all 288 Unix timestamps for 5-min windows on a given UTC day."""
    target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    day_start_unix = int(target.timestamp())
    return [day_start_unix + i * 300 for i in range(288)]


def fetch_events_by_slugs(
    client: niquests.Session,
    slugs: list[str],
) -> tuple[list[dict], list[str]]:
    """Fetch events from Gamma API by slug in batches.

    Returns (events, failed_batches).
    """
    all_events: list[dict] = []
    failed_batches: list[str] = []
    n_batches = (len(slugs) + SLUG_BATCH_SIZE - 1) // SLUG_BATCH_SIZE

    for i in range(0, len(slugs), SLUG_BATCH_SIZE):
        batch = slugs[i : i + SLUG_BATCH_SIZE]
        results: list = []

        for attempt in range(MAX_RETRIES):
            try:
                resp = client.get(
                    f"{GAMMA_BASE}/events",
                    params={"slug": batch, "closed": "true"},
                    timeout=30.0,
                )
                resp.raise_for_status()
                results = orjson.loads(resp.content)
                if isinstance(results, list):
                    all_events.extend(results)
                break
            except Exception as exc:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    print(f"      Retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {exc}")
                    time.sleep(delay)
                else:
                    print(f"      FAILED slug batch: {exc}")
                    failed_batches.append(f"batch_{i // SLUG_BATCH_SIZE + 1}")

        batch_num = i // SLUG_BATCH_SIZE + 1
        print(
            f"    Slug batch {batch_num}/{n_batches}: "
            f"got {len(results)} events (running total: {len(all_events)})"
        )
        time.sleep(GAMMA_DELAY)

    return all_events, failed_batches


def fetch_all_events_for_day(
    client: niquests.Session,
    date_str: str,
    slug_prefixes: list[str],
) -> tuple[list[dict], list[str]]:
    """Fetch all events for a UTC day across multiple slug prefixes.

    Returns (deduplicated_events, failed_batches).
    """
    all_events: list[dict] = []
    seen_slugs: set[str] = set()
    all_failures: list[str] = []

    timestamps = compute_5min_timestamps(date_str)

    for prefix in slug_prefixes:
        slugs = [f"{prefix}{ts}" for ts in timestamps]
        events, failures = fetch_events_by_slugs(client, slugs)
        all_failures.extend(failures)

        for ev in events:
            slug = ev.get("slug", "")
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                all_events.append(ev)

    all_events.sort(key=lambda e: e.get("slug") or "")
    return all_events, all_failures


def extract_token_maps(events: list[dict]) -> tuple[dict[int, int], dict[int, str]]:
    """Extract token ID -> event_id and token ID -> outcome mappings.

    Returns:
        (token_to_event_id, token_to_outcome)
    """
    token_to_event: dict[int, int] = {}
    token_to_outcome: dict[int, str] = {}

    for event in events:
        event_id = event.get("id")
        if event_id is None:
            continue

        markets = event.get("markets", [])
        if isinstance(markets, str):
            markets = orjson.loads(markets)

        for market in markets:
            tokens = market.get("clobTokenIds", [])
            if isinstance(tokens, str):
                tokens = orjson.loads(tokens)

            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str):
                outcomes = orjson.loads(outcomes)

            if not isinstance(tokens, list):
                continue

            for i, token in enumerate(tokens):
                token_int = int(token)
                token_to_event[token_int] = int(event_id)
                if isinstance(outcomes, list) and i < len(outcomes):
                    token_to_outcome[token_int] = outcomes[i]

    return token_to_event, token_to_outcome
