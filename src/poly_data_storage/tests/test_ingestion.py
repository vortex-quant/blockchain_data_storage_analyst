"""Focused regressions for lossless continuation and unchanged output columns."""

from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import niquests
import orjson
import pyarrow.parquet as pq

from poly_data_storage import cli, sqd_portal
from poly_data_storage.block_timestamp_resolve import resolve_block_range
from poly_data_storage.blockchain_decoder import DecodeError, decode_order_filled
from poly_data_storage.constants import (
    EXCHANGES,
    MAX_RETRIES,
    ORDER_FILLED_TOPIC,
    ORDER_FILLED_V1_TOPIC,
)
from poly_data_storage.storage import (
    COMPLETION_KEY,
    ORDER_FILLS_SCHEMA,
    is_complete_file,
)


def event(address: str, *, sell: bool = False, index: int = 0) -> dict:
    legacy = EXCHANGES[address] == "v1"
    words = (
        (
            [123, 0, 1_000_000, 500_000, 100]
            if sell
            else [0, 123, 500_000, 1_000_000, 100]
        )
        if legacy
        else [
            int(sell),
            123,
            1_000_000 if sell else 500_000,
            500_000 if sell else 1_000_000,
            100,
            0,
            0,
        ]
    )
    return {
        "address": address,
        "topics": [
            ORDER_FILLED_V1_TOPIC if legacy else ORDER_FILLED_TOPIC,
            "0x" + "11" * 32,
            "0x" + "00" * 12 + "22" * 20,
            "0x" + "00" * 12 + address[2:],
        ],
        "data": "0x" + "".join(f"{word:064x}" for word in words),
        "transactionHash": "0x" + "33" * 32,
        "logIndex": index,
    }


def block(number: int, *, timestamp: int = 1000, logs: list | None = None) -> bytes:
    return orjson.dumps(
        {
            "header": {
                "number": number,
                "timestamp": timestamp,
                "hash": "0x" + "aa" * 32,
            },
            "logs": logs or [],
        }
    )


def response(
    lines: list[bytes] | None = None,
    *,
    status: int = 200,
    error: Exception | None = None,
) -> MagicMock:
    resp = MagicMock(spec=niquests.Response)
    resp.status_code = status
    resp.headers = {}

    def iterate():
        yield from lines or []
        if error:
            raise error

    resp.iter_lines.side_effect = iterate
    return resp


class IngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        sleeper = patch("poly_data_storage.sqd_portal.time.sleep")
        sleeper.start()
        self.addCleanup(sleeper.stop)
        for target in ("poly_data_storage.sqd_portal.log", "poly_data_storage.cli.log"):
            logger = patch(target)
            logger.start()
            self.addCleanup(logger.stop)
        self.client = MagicMock(spec=niquests.Session)
        self.address = next(a for a, version in EXCHANGES.items() if version == "v2")

    def collect(self, first: int, last: int, *, batch_size: int = 2) -> list:
        return [
            row
            for batch in sqd_portal.stream_decoded_logs(
                self.client, first, last, 1000, 2000, batch_size
            )
            for row in batch
        ]

    def test_all_deployments_decode_buy_sell_to_same_columns(self) -> None:
        for address in EXCHANGES:
            for sell in (False, True):
                with self.subTest(address=address, sell=sell):
                    row = decode_order_filled(event(address, sell=sell), 1, 1000)
                    self.assertEqual(list(row), ORDER_FILLS_SCHEMA.names)
                    self.assertEqual(
                        (row["amount_usd"], row["shares"], row["price"]),
                        (0.5, 1.0, 0.5),
                    )
                    self.assertEqual(row["is_sell"], sell)
                    self.assertTrue(row["is_taker"])
                    self.assertEqual(row["fee"], 0.0001)
        maker = event(self.address)
        maker["topics"][3] = "0x" + "00" * 12 + "44" * 20
        self.assertFalse(decode_order_filled(maker, 1, 1000)["is_taker"])

    def test_invalid_layout_identity_and_side_fail(self) -> None:
        for change in (
            {"logIndex": None},
            {"transactionHash": None},
            {"data": "0x00"},
            {"topics": []},
        ):
            with self.subTest(change=change), self.assertRaises(DecodeError):
                decode_order_filled(dict(event(self.address), **change), 1, 1000)
        raw = event(self.address)
        raw["data"] = "0x" + f"{2:064x}" + raw["data"][66:]
        with self.assertRaises(DecodeError):
            decode_order_filled(raw, 1, 1000)

    def test_header_only_partial_response_cannot_skip_later_fills(self) -> None:
        self.client.post.side_effect = [
            response([block(0, logs=[event(self.address)]), block(1)]),
            response([block(2), block(3)]),
            response([block(4, logs=[event(self.address, index=1)]), block(5)]),
        ]
        rows = self.collect(0, 5)
        self.assertEqual([row["block_number"] for row in rows], [0, 4])
        self.assertEqual(
            [c.kwargs["json"]["fromBlock"] for c in self.client.post.call_args_list],
            [0, 2, 4],
        )
        self.assertTrue(
            all(
                c.kwargs["json"]["includeAllBlocks"]
                for c in self.client.post.call_args_list
            )
        )

    def test_malformed_line_and_read_failure_resume_without_duplicates(self) -> None:
        self.client.post.side_effect = [
            response([block(0, logs=[event(self.address)]), b"{broken"]),
            response([block(1)], error=niquests.exceptions.ConnectionError("reset")),
            response([block(2, logs=[event(self.address, index=1)])]),
        ]
        self.assertEqual(
            [r["block_number"] for r in self.collect(0, 2, batch_size=1)], [0, 2]
        )
        self.assertEqual(
            [c.kwargs["json"]["fromBlock"] for c in self.client.post.call_args_list],
            [0, 1, 2],
        )

    def test_http_200_failures_and_empty_responses_exhaust_budget(self) -> None:
        for lines, error in (
            ([], None),
            ([], niquests.exceptions.ConnectionError("reset")),
            ([b"{}"], None),
            ([block(1)], None),
        ):
            with self.subTest(lines=lines, error=error):
                self.client.reset_mock()
                self.client.post.side_effect = (
                    lambda *a, lines=lines, error=error, **kw: response(
                        lines, error=error
                    )
                )
                with self.assertRaises(
                    (
                        sqd_portal.RetryablePortalError,
                        niquests.exceptions.RequestException,
                    )
                ):
                    self.collect(0, 2)
                self.assertEqual(self.client.post.call_count, MAX_RETRIES + 1)

    def test_204_and_duplicate_logs_do_not_pass(self) -> None:
        self.client.post.return_value = response(status=204)
        with self.assertRaises(sqd_portal.DayNotReady):
            self.collect(0, 2)
        self.client.post.return_value = response(
            [block(0, logs=[event(self.address), event(self.address)])]
        )
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.collect(0, 0)

    def test_retry_after_and_permanent_errors(self) -> None:
        busy = response(status=529)
        busy.headers = {"Retry-After": "7"}
        busy.json.return_value = {"error": {"type": "rate_limit_error"}}
        self.client.post.side_effect = [busy, response([block(0)])]
        with patch("poly_data_storage.sqd_portal.time.sleep") as sleep:
            self.collect(0, 0)
            self.assertIn(call(7.0), sleep.call_args_list)
        self.client.reset_mock()
        permanent = response(status=400)
        permanent.json.return_value = {"error": {"type": "invalid_request_error"}}
        self.client.post.side_effect = None
        self.client.post.return_value = permanent
        with self.assertRaises(RuntimeError):
            self.collect(0, 0)
        self.assertEqual(self.client.post.call_count, 1)

    def test_exact_midnights_cached_and_unfinalized_day_rejected(self) -> None:
        start = int(datetime(2026, 7, 9, tzinfo=UTC).timestamp())
        cache = {}
        evidence = {}

        def json_endpoint(client, url, **kwargs):
            if url.endswith("finalized-head"):
                return {"number": 1000, "hash": "0x" + "aa" * 32}
            ts = int(url.split("/")[-2])
            return {"block_number": 100 + (ts - start) // 86400 * 10}

        def headers(client, first, last, **kwargs):
            ts = start + (last - 100) // 10 * 86400
            yield orjson.loads(block(first, timestamp=ts - 1))
            yield orjson.loads(block(last, timestamp=ts + 1))

        with (
            patch(
                "poly_data_storage.block_timestamp_resolve.get_json",
                side_effect=json_endpoint,
            ) as get,
            patch(
                "poly_data_storage.block_timestamp_resolve.stream_blocks",
                side_effect=headers,
            ),
        ):
            self.assertEqual(
                resolve_block_range(
                    self.client, "2026-07-09", boundary_cache=cache, evidence=evidence
                ),
                (100, 109, start, start + 86400),
            )
            resolve_block_range(self.client, "2026-07-10", boundary_cache=cache)
            self.assertEqual(
                sum("timestamps" in c.args[1] for c in get.call_args_list), 3
            )
        with (
            patch(
                "poly_data_storage.block_timestamp_resolve.get_json",
                return_value={"number": 109},
            ),
            self.assertRaises(sqd_portal.DayNotReady),
        ):
            resolve_block_range(self.client, "2026-07-09", boundary_cache=cache)

    def test_midnight_validation_and_404_fail_without_guessed_range(self) -> None:
        with (
            patch(
                "poly_data_storage.block_timestamp_resolve.get_json",
                side_effect=[
                    {"block_number": 10},
                    {"block_number": 20},
                    {"number": 100},
                ],
            ),
            patch(
                "poly_data_storage.block_timestamp_resolve.stream_blocks",
                return_value=iter(
                    [
                        orjson.loads(block(9, timestamp=0)),
                        orjson.loads(block(10, timestamp=1)),
                    ]
                ),
            ),
            self.assertRaisesRegex(ValueError, "first block"),
        ):
            resolve_block_range(self.client, "2026-07-09")
        self.client.get.return_value = response(status=404)
        with self.assertRaises(sqd_portal.DayNotReady):
            sqd_portal.get_json(self.client, "timestamp", timestamp_lookup=True)
        self.assertEqual(self.client.get.call_count, 1)

    def test_daily_publication_footer_skip_and_failure_cleanup(self) -> None:
        def resolve(client, date, **kwargs):
            kwargs["evidence"].update(
                start_block=0,
                end_block=1,
                next_day_block=2,
                finalized_head=2,
                day_start_ts=1000,
                day_end_ts=2000,
            )
            return 0, 1, 1000, 2000

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(cli, "OUTPUT_DIR", Path(directory)),
            patch.object(cli, "resolve_block_range", side_effect=resolve),
        ):
            self.client.post.return_value = response(
                [block(0, logs=[event(self.address)]), block(1)]
            )
            self.assertTrue(cli.run_for_day(self.client, "2026-07-09"))
            path = Path(directory) / "polymarket_orders_2026_07_09.parquet"
            self.assertTrue(is_complete_file(path, "2026-07-09"))
            table = pq.read_table(path)
            self.assertTrue(
                table.schema.equals(ORDER_FILLS_SCHEMA, check_metadata=False)
            )
            self.assertEqual(table.num_rows, 1)
            self.assertIn(COMPLETION_KEY.encode(), pq.read_metadata(path).metadata)
            self.assertTrue(cli.run_for_day(self.client, "2026-07-09"))
            self.assertEqual(self.client.post.call_count, 1)
            self.client.post.return_value = response(status=204)
            self.assertIsNone(cli.run_for_day(self.client, "2026-07-10"))
            self.assertFalse(
                (Path(directory) / "polymarket_orders_2026_07_10.parquet").exists()
            )
            self.assertEqual(list(Path(directory).glob(".tmp_*")), [])
            legacy = Path(directory) / "polymarket_orders_2026_07_11.parquet"
            pq.write_table(table.replace_schema_metadata(None), legacy)
            self.assertFalse(cli.run_for_day(self.client, "2026-07-11"))
            # Even after a real batch was written, a bad later block must not
            # replace the previous good file or leave a publishable partial file.
            original = path.read_bytes()
            self.client.post.return_value = response(
                [
                    block(0, logs=[event(self.address)]),
                    block(1, logs=[dict(event(self.address), logIndex=None)]),
                ]
            )
            with (
                patch.object(cli, "REPLACE", True),
                patch.object(
                    cli,
                    "stream_decoded_logs",
                    partial(sqd_portal.stream_decoded_logs, batch_size=1),
                ),
            ):
                self.assertFalse(cli.run_for_day(self.client, "2026-07-09"))
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(list(Path(directory).glob(".tmp_*")), [])
            self.client.post.return_value = response([block(0), block(1)])
            self.assertTrue(cli.run_for_day(self.client, "2026-07-12"))
            empty = Path(directory) / "polymarket_orders_2026_07_12.parquet"
            self.assertEqual(pq.read_metadata(empty).num_rows, 0)
            self.assertTrue(is_complete_file(empty, "2026-07-12"))

    def test_competing_daily_writer_is_rejected(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(cli, "OUTPUT_DIR", Path(directory)),
            (Path(directory) / ".polymarket_orders_2026_07_09.parquet.lock").open(
                "a"
            ) as lock,
        ):
            cli.fcntl.flock(lock, cli.fcntl.LOCK_EX | cli.fcntl.LOCK_NB)
            self.assertFalse(cli.run_for_day(self.client, "2026-07-09"))
            self.client.post.assert_not_called()

    def test_valid_empty_day_and_invalid_dates(self) -> None:
        self.client.post.return_value = response([block(0), block(1)])
        self.assertEqual(self.collect(0, 1), [])
        with self.assertRaises(ValueError):
            cli.daterange("2026-07-10", "2026-07-09")


if __name__ == "__main__":
    unittest.main()
