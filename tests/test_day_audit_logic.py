"""Offline regressions for the audit's evidence and matching rules."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import niquests
import polars as pl
from audit_day_completeness import (
    compare_details,
    fetch_market,
    scan_chain,
    validate_page,
)

from poly_data_storage.constants import ORDER_FILLED_TOPIC


def trade(*, size: float = 10, wallet: str = "0xaa") -> dict:
    return {
        "transaction_hash": "0x11",
        "token_id": "123",
        "condition_id": "0xcc",
        "side": "BUY",
        "proxy_wallet": wallet,
        "price": 0.5,
        "size": size,
        "timestamp": 100,
    }


def fill() -> dict:
    return {
        "tx_hash": "0x11",
        "token_id": "123",
        "side": "BUY",
        "maker": "0xaa",
        "price": 0.5,
        "shares": 10,
        "timestamp": 100,
        "fill_role": "taker_aggregate",
    }


def page(rows: list, cursor: str | None) -> dict:
    return {
        "data": rows,
        "pagination": {"has_more": cursor is not None, "next_cursor": cursor},
    }


class AuditLogicTests(unittest.TestCase):
    def test_transaction_presence_does_not_prove_detail_match(self) -> None:
        result = compare_details([fill()], [trade(size=9)])
        self.assertEqual(result["api_transactions_absent_from_file"], 0)
        self.assertEqual(result["unmatched_detail_rows"], 1)

    def test_candidates_are_consumed_once(self) -> None:
        result = compare_details([fill()], [trade(), trade()])
        self.assertEqual(result["matched_detail_rows"], 1)
        self.assertEqual(result["unmatched_detail_rows"], 1)

    def test_wallet_side_token_and_timestamp_are_part_of_match(self) -> None:
        for changed in (
            {"proxy_wallet": "0xbb"},
            {"side": "SELL"},
            {"token_id": "456"},
            {"timestamp": 101},
        ):
            with self.subTest(changed=changed):
                result = compare_details([fill()], [dict(trade(), **changed)])
                self.assertEqual(result["unmatched_detail_rows"], 1)

    def test_duplicate_economic_fingerprints_are_reported(self) -> None:
        result = compare_details([fill(), fill()], [trade()])
        self.assertEqual(result["ambiguous_matches"], 1)
        self.assertEqual(result["file_aggregate_rows_unmatched_by_api"], 1)

    def test_empty_api_does_not_erase_reverse_difference(self) -> None:
        result = compare_details([fill()], [])
        self.assertEqual(result["file_aggregate_rows_unmatched_by_api"], 1)

    def test_malformed_pagination_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            validate_page([])
        with self.assertRaises(ValueError):
            validate_page(
                {"data": [], "pagination": {"has_more": True, "next_cursor": None}}
            )

    @patch("audit_day_completeness.time.sleep")
    @patch("audit_day_completeness.get_json")
    def test_short_and_empty_nonterminal_pages_continue(
        self, get: MagicMock, _sleep: MagicMock
    ) -> None:
        get.side_effect = [
            page([dict(trade(), timestamp=200)], "a"),
            page([], "b"),
            page([trade()], None),
        ]
        with tempfile.TemporaryDirectory() as name:
            records, evidence = fetch_market(
                niquests.Session(), "0xcc", 100, 200, Path(name), 5
            )
        self.assertEqual(len(records), 1)
        self.assertTrue(evidence["terminal_cursor_reached"])
        self.assertEqual(get.call_count, 3)
        self.assertEqual(get.call_args.args[2]["condition"], "0xcc")

    @patch("audit_day_completeness.time.sleep")
    @patch("audit_day_completeness.get_json")
    def test_budget_never_implies_complete(
        self, get: MagicMock, _sleep: MagicMock
    ) -> None:
        get.return_value = page([trade()], "a")
        with tempfile.TemporaryDirectory() as name:
            _, evidence = fetch_market(
                niquests.Session(), "0xcc", 100, 200, Path(name), 1
            )
        self.assertFalse(evidence["terminal_cursor_reached"])

    @patch("audit_day_completeness.time.sleep")
    @patch("audit_day_completeness.get_json")
    def test_repeated_cursor_fails(self, get: MagicMock, _sleep: MagicMock) -> None:
        get.return_value = page([trade()], "a")
        with tempfile.TemporaryDirectory() as name, self.assertRaises(ValueError):
            fetch_market(niquests.Session(), "0xcc", 100, 200, Path(name), 5)

    @patch("audit_day_completeness.stream_blocks")
    @patch("audit_day_completeness.resolve_block_range")
    def test_chain_reconciliation_counts_missing_and_extra_per_log(
        self, boundary: MagicMock, stream: MagicMock
    ) -> None:
        address = "0xe111180000d2663c0091e4f400237545b87b996b"
        tx_hash = "0x" + "33" * 32
        raw = {
            "address": address,
            "topics": [
                ORDER_FILLED_TOPIC,
                "0x" + "11" * 32,
                "0x" + "00" * 12 + "22" * 20,
                "0x" + "00" * 12 + address[2:],
            ],
            "data": "0x"
            + "".join(f"{v:064x}" for v in [0, 123, 500000, 1000000, 0, 0, 0]),
            "transactionHash": tx_hash,
            "logIndex": 0,
        }
        blocks = [
            {
                "header": {"number": 1, "timestamp": 100},
                "logs": [raw, dict(raw, logIndex=2)],
            },
            {"header": {"number": 2, "timestamp": 101}, "logs": []},
        ]
        boundary.return_value = (1, 2, 100, 200)
        stream.return_value = (b for b in blocks)
        with tempfile.TemporaryDirectory() as name:
            folder = Path(name)
            path = folder / "fills.parquet"
            pl.DataFrame(
                {
                    "block_number": [1, 1],
                    "tx_hash": [tx_hash, tx_hash],
                    "log_index": [0, 1],
                }
            ).write_parquet(path)
            result = scan_chain(niquests.Session(), path, "2026-07-12", folder)
        self.assertEqual(result["blocks_scanned"], 2)
        self.assertEqual(result["source_logs_by_exchange"][address], 2)
        self.assertEqual(result["missing_logs_by_exchange"][address], 1)
        self.assertEqual(result["local_logs_absent_from_sqd"], 1)
        self.assertEqual(result["missing_examples"][0]["decoded"]["log_index"], 2)


if __name__ == "__main__":
    unittest.main()
