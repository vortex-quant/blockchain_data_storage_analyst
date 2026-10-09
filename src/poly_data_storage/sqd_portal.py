"""Finalized SQD streams with one cursor, bounded retries and exact progress."""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from math import ceil, isfinite, log2
from typing import Any

import niquests
import orjson

from poly_data_storage.blockchain_decoder import OrderFill, decode_order_filled
from poly_data_storage.constants import (
    EXCHANGES,
    MAX_RETRIES,
    ORDER_FILLED_TOPIC,
    ORDER_FILLED_V1_TOPIC,
    RETRY_BASE_DELAY,
    RETRY_MAX_DELAY,
    SERVICE_RETRY_TIMEOUT,
    SQD_DELAY,
    SQD_URL,
    WRITE_BATCH_SIZE,
)
from poly_data_storage.logger import get_logger

log = get_logger()
EST = timezone(timedelta(hours=-5), "EST")
LOG_FILTERS = [
    {
        "address": [a for a, v in EXCHANGES.items() if v == "v1"],
        "topic0": [ORDER_FILLED_V1_TOPIC],
    },
    {
        "address": [a for a, v in EXCHANGES.items() if v != "v1"],
        "topic0": [ORDER_FILLED_TOPIC],
    },
]


class DayNotReady(RuntimeError):
    """The full requested day is not available as finalized history yet."""


class RetryablePortalError(RuntimeError):
    def __init__(
        self,
        message: str,
        retry_after: str | None = None,
        *,
        service_error: bool = False,
    ) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.service_error = service_error


def _check_response(resp: niquests.Response) -> None:
    status = resp.status_code
    if status is None:
        raise RetryablePortalError("SQD response has no HTTP status")
    if status == 204:
        raise DayNotReady("SQD returned 204 before the requested range was complete")
    if status < 400:
        if status != 200:
            raise RuntimeError(f"Unexpected SQD HTTP status: {status}")
        return
    try:
        error = resp.json().get("error", {})
    except ValueError, AttributeError:
        error = {}
    message = f"SQD HTTP {resp.status_code}: {error}"
    if error.get("type") in {"rate_limit_error", "availability_error"} or (
        not error.get("type") and resp.status_code in {429, 502, 503, 504, 529}
    ):
        raise RetryablePortalError(
            message, resp.headers.get("Retry-After"), service_error=True
        )
    raise RuntimeError(message)


def _retry_delay(retry_after: str | None, failures: int) -> float:
    """Use the server interval directly; cap only our fallback backoff."""
    if retry_after:
        try:
            delay = float(retry_after)
        except ValueError:
            try:
                when = parsedate_to_datetime(retry_after)
                delay = max(0.0, (when - datetime.now(UTC)).total_seconds())
            except TypeError, ValueError, OverflowError:
                delay = -1.0
        if isfinite(delay) and delay >= 0:
            return delay
    # Limit the exponent as well, even after many server-directed retries.
    exponent = min(failures - 1, max(0, ceil(log2(RETRY_MAX_DELAY / RETRY_BASE_DELAY))))
    return min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * 2**exponent)


class _RetryState:
    """Separate service capacity waits from bounded response/transport retries."""

    def __init__(self) -> None:
        self.progress()

    def progress(self) -> None:
        self.failures = self.service_failures = 0
        self.last_progress = time.monotonic()

    def wait(self, exc: Exception) -> None:
        service = isinstance(exc, RetryablePortalError) and exc.service_error
        if service:
            self.service_failures += 1
            failures = self.service_failures
            remaining = SERVICE_RETRY_TIMEOUT - (time.monotonic() - self.last_progress)
            if remaining <= 0:
                raise TimeoutError(
                    "SQD service retry budget exhausted without progress"
                ) from exc
        else:
            self.failures += 1
            failures = self.failures
            if failures > MAX_RETRIES:
                raise exc
        delay = _retry_delay(getattr(exc, "retry_after", None), failures)
        if service:
            log.warning(
                "  SQD service retry %s after %.1fs (%.1fs budget remaining): %s",
                failures,
                delay,
                remaining,
                exc,
            )
            # If Retry-After exceeds our remaining budget, wait out the budget
            # and fail. Never issue an early retry by shortening the header.
            time.sleep(min(delay, remaining))
            if time.monotonic() - self.last_progress >= SERVICE_RETRY_TIMEOUT:
                raise TimeoutError(
                    "SQD service retry budget exhausted without progress"
                ) from exc
        else:
            log.warning(
                "  SQD retry %s/%s after %.1fs: %s", failures, MAX_RETRIES, delay, exc
            )
            time.sleep(delay)


def get_json(
    client: niquests.Session, url: str, *, timestamp_lookup: bool = False
) -> Any:
    """Read a small SQD JSON endpoint with the same retry/error policy."""
    retries = _RetryState()
    while True:
        resp = None
        try:
            resp = client.get(url, timeout=30.0)
            if timestamp_lookup and resp.status_code == 404:
                raise DayNotReady("No block at or after the requested UTC boundary yet")
            _check_response(resp)
            try:
                return resp.json()
            except ValueError as exc:
                raise RetryablePortalError("Malformed SQD JSON response") from exc
        except (niquests.exceptions.RequestException, RetryablePortalError) as exc:
            # Close before waiting, including on overloaded streamed responses.
            if resp is not None:
                resp.close()
            retries.wait(exc)
        finally:
            if resp is not None:
                resp.close()


def _format_block_time(timestamp: int) -> str:
    """Show the header time in UTC and fixed EST, including both dates."""
    utc = datetime.fromtimestamp(timestamp, tz=UTC)
    return f"{utc:%d_%m_%Y %H:%M:%S} UTC {utc.astimezone(EST):%d_%m_%Y %H:%M:%S} EST"


def stream_blocks(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    *,
    include_logs: bool = True,
) -> Generator[dict[str, Any]]:
    """Yield consecutive complete blocks, including blocks without matching logs.

    The cursor advances after the consumer processes a block. A response ending
    normally is only a batch boundary. Empty/truncated responses never certify
    completion. includeAllBlocks makes every expected block explicit.
    """
    if start_block < 0 or end_block < start_block - 1:
        raise ValueError("Invalid inclusive block range")
    current = start_block
    retries = _RetryState()
    while current <= end_block:
        request_start = current
        first_timestamp = last_timestamp = 0
        fields: dict[str, Any] = {
            "block": {"number": True, "timestamp": True, "hash": True}
        }
        if include_logs:
            fields["log"] = {
                "address": True,
                "topics": True,
                "data": True,
                "transactionHash": True,
                "logIndex": True,
            }
        payload = {
            "type": "evm",
            "fromBlock": current,
            "toBlock": end_block,
            "includeAllBlocks": True,
            "fields": fields,
            "logs": LOG_FILTERS if include_logs else [],
        }
        resp = None
        try:
            log.info("  Requesting finalized blocks %s-%s...", current, end_block)
            resp = client.post(SQD_URL, json=payload, timeout=120.0, stream=True)
            _check_response(resp)
            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                except ValueError as exc:
                    raise RetryablePortalError(
                        f"Malformed block JSON at cursor {current}"
                    ) from exc
                if not isinstance(obj, dict) or not isinstance(obj.get("header"), dict):
                    raise RetryablePortalError(
                        f"Missing block header at cursor {current}"
                    )
                header = obj["header"]
                number, timestamp = header.get("number"), header.get("timestamp")
                if type(number) is not int or number != current or number > end_block:
                    raise RetryablePortalError(
                        f"Expected block {current}, received {number}"
                    )
                if type(timestamp) is not int or timestamp < 0:
                    raise RetryablePortalError(f"Invalid timestamp at block {number}")
                if not isinstance(obj.get("logs", []), list):
                    raise RetryablePortalError(f"Invalid logs at block {number}")
                yield obj
                if number == request_start:
                    first_timestamp = timestamp
                last_timestamp = timestamp
                current = number + 1
                retries.progress()
            if current == request_start:
                raise RetryablePortalError(
                    f"Empty response before block {current} was scanned"
                )
            log.info(
                "  Processed blocks %s-%s (%s) - (%s)",
                request_start,
                current - 1,
                _format_block_time(first_timestamp),
                _format_block_time(last_timestamp),
            )
        except (niquests.exceptions.RequestException, RetryablePortalError) as exc:
            if resp is not None:
                resp.close()
            retries.wait(exc)
            continue
        finally:
            if resp is not None:
                resp.close()
        if current <= end_block:
            time.sleep(SQD_DELAY)


@dataclass
class ScanStats:
    blocks: int = 0
    matching_logs: int = 0
    decoded_rows: int = 0


def stream_decoded_logs(
    client: niquests.Session,
    start_block: int,
    end_block: int,
    day_start_ts: int,
    day_end_ts: int,
    batch_size: int = WRITE_BATCH_SIZE,
    *,
    stats: ScanStats | None = None,
) -> Generator[list[OrderFill]]:
    """Yield the unchanged row schema; any rejected source log fails the day."""
    if batch_size <= 0 or day_start_ts >= day_end_ts:
        raise ValueError("Invalid batch size or UTC time interval")
    stats = stats if stats is not None else ScanStats()
    batch: list[OrderFill] = []
    with closing(stream_blocks(client, start_block, end_block)) as blocks:
        for block in blocks:
            number, timestamp = block["header"]["number"], block["header"]["timestamp"]
            if not day_start_ts <= timestamp < day_end_ts:
                raise ValueError(f"Block {number} is outside the verified UTC day")
            # Validate the entire block before yielding any batch containing it.
            rows = [
                decode_order_filled(entry, number, timestamp)
                for entry in block.get("logs", [])
            ]
            indices = [row["log_index"] for row in rows]
            if len(indices) != len(set(indices)):
                raise ValueError(f"Duplicate log identity in block {number}")
            stats.blocks += 1
            stats.matching_logs += len(rows)
            stats.decoded_rows += len(rows)
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
    if batch:
        yield batch
