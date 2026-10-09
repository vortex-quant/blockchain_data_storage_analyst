"""Read-only live audit of a daily fill file; never infer completeness from a sample.

uv run --no-sync python tests/audit_day_completeness.py --chain-scan

Outputs evidence only under --output. Existing source and datasets are untouched.
Exit 1 = discrepancies, 2 = inconclusive/incomplete audit, 0 = full SQD identity
reconciliation plus selected API detail checks (not independent chain proof).
"""

from __future__ import annotations

import argparse
import hashlib
import math
import time
from collections import Counter, defaultdict
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import niquests
import orjson
import polars as pl
import pyarrow.parquet as pq

from poly_data_storage.block_timestamp_resolve import resolve_block_range
from poly_data_storage.blockchain_decoder import decode_order_filled
from poly_data_storage.constants import EXCHANGES
from poly_data_storage.sqd_portal import stream_blocks
from poly_data_storage.storage import is_complete_file

ROOT = Path(__file__).resolve().parents[1]
API = "https://data-api.polymarket.com"
BTC_TOKEN = (
    "41590868698844731693919196382535076142473982008228244562680705896443905539787"
)


def get_json(client: niquests.Session, url: str, params: dict[str, str]) -> Any:
    """Retry transient responses; a permanent error never means an empty page."""
    for attempt in range(5):
        try:
            with client.get(url, params=params, timeout=40) as response:
                if response.status_code in {429, 500, 502, 503, 504, 529}:
                    wait = response.headers.get("Retry-After")
                    try:
                        delay = float(wait) if wait else 2**attempt
                    except ValueError:
                        delay = 2**attempt
                    if not math.isfinite(delay) or delay < 0:
                        delay = 2**attempt
                    if attempt == 4:
                        response.raise_for_status()
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                return response.json()
        except niquests.exceptions.RequestException:
            if attempt == 4:
                raise
            time.sleep(2**attempt)
    raise RuntimeError("API retry budget exhausted")


def validate_page(payload: Any) -> tuple[list[dict[str, Any]], str | None]:
    """Short/empty pages with a next cursor are not terminal."""
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise TypeError("Malformed v2 trade envelope")
    pagination = payload.get("pagination")
    if not isinstance(pagination, dict):
        raise TypeError("Missing pagination evidence")
    more, cursor = pagination.get("has_more"), pagination.get("next_cursor")
    if type(more) is not bool or (cursor is not None and not isinstance(cursor, str)):
        raise ValueError("Invalid pagination evidence")
    if more != (cursor is not None) or cursor == "":
        raise ValueError("Contradictory pagination evidence")
    return payload["data"], cursor


def fetch_market(
    client: niquests.Session,
    condition: str,
    start: int,
    end: int,
    output: Path,
    max_pages: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Walk market history with stable filters, then retain the half-open UTC day.

    No reliance on market-scoped start/end (ignored by the v2 serving shape).
    Fetch all pages to terminal cursor; a page budget is explicitly inconclusive.
    A tiny positive filter avoids the API's documented default 0.01-share floor.
    """
    params = {
        "condition": condition,
        "limit": "1000",
        "taker_only": "true",
        "filter_type": "TOKENS",
        "filter_amount": "0.000001",
    }
    trades: list[dict[str, Any]] = []
    cursors: set[str] = set()
    total = pages = 0
    terminal = False
    previous_ts: int | None = None
    raw_path = output / f"api_{condition}.ndjson"
    with raw_path.open("wb") as raw:
        for _ in range(max_pages):
            payload = get_json(client, f"{API}/v2/trades", params)
            raw.write(
                orjson.dumps({"request": params.copy(), "response": payload}) + b"\n"
            )
            data, cursor = validate_page(payload)
            pages += 1
            total += len(data)
            for trade in data:
                if (
                    not isinstance(trade, dict)
                    or trade.get("condition_id") != condition
                ):
                    raise ValueError("Unexpected market or invalid trade row")
                timestamp = trade.get("timestamp")
                if type(timestamp) is not int:
                    raise ValueError("Invalid API timestamp")
                if previous_ts is not None and timestamp > previous_ts:
                    raise ValueError("API history is not timestamp descending")
                previous_ts = timestamp
                if start <= timestamp < end:
                    trades.append(trade)
            if cursor is None:
                terminal = True
                break
            if cursor in cursors:
                raise ValueError("API cursor repeated without termination")
            cursors.add(cursor)
            params["cursor"] = cursor
            time.sleep(0.15)
    return trades, {
        "pages": pages,
        "history_rows": total,
        "day_rows": len(trades),
        "terminal_cursor_reached": terminal,
        "raw_evidence": str(raw_path),
        "minimum_shares_requested": 0.000001,
    }


def canonical_fills(path: Path) -> pl.LazyFrame:
    frame = pl.scan_parquet(path)
    names = frame.collect_schema().names()
    if "side" not in names:
        frame = frame.with_columns(
            pl.when("is_sell")
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("BUY"))
            .alias("side")
        )
    if "fill_role" not in names:
        frame = frame.with_columns(
            pl.when("is_taker")
            .then(pl.lit("taker_aggregate"))
            .otherwise(pl.lit("maker"))
            .alias("fill_role")
        )
    return frame.with_columns(pl.col("tx_hash", "maker", "taker").str.to_lowercase())


def compare_details(
    fills: list[dict[str, Any]], trades: list[dict[str, Any]]
) -> dict[str, Any]:
    """Consume candidates once: a transaction match cannot hide a missing fill.

    API lacks log_index/order_hash. Ambiguities are reported, never exact log proof.
    Price tolerance is 1e-8; size tolerance is 1e-6 shares (native microshare).
    """
    candidates: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(
        list
    )
    for row in fills:
        candidates[(row["tx_hash"], row["token_id"], row["side"], row["maker"])].append(
            row
        )
    matched = ambiguous = 0
    roles: Counter[str] = Counter()
    failures: list[dict[str, Any]] = []
    missing = 0
    for trade in trades:
        key = (
            trade["transaction_hash"].lower(),
            trade["token_id"],
            trade["side"],
            trade["proxy_wallet"].lower(),
        )
        price, size = float(trade["price"]), float(trade["size"])
        if not math.isfinite(price) or not math.isfinite(size) or size <= 0:
            raise ValueError("Invalid API amounts")
        bucket = candidates.get(key, [])
        found = [
            i
            for i, row in enumerate(bucket)
            if row["price"] is not None
            and abs(row["price"] - price) <= 1e-8
            and abs(row["shares"] - size) <= 1e-6
            and row["timestamp"] == trade["timestamp"]
        ]
        if found:
            ambiguous += int(len(found) > 1)
            row = bucket.pop(found[0])
            matched += 1
            roles[row["fill_role"]] += 1
        else:
            missing += 1
            if len(failures) < 10:
                failures.append({"api_trade": trade, "candidate_fills": bucket[:3]})
    api_tx = {t["transaction_hash"].lower() for t in trades}
    bc_tx = {r["tx_hash"] for r in fills}
    return {
        "api_rows": len(trades),
        "matched_detail_rows": matched,
        "unmatched_detail_rows": missing,
        "ambiguous_matches": ambiguous,
        "matched_roles": dict(roles),
        "api_transactions_absent_from_file": len(api_tx - bc_tx),
        "file_transactions_absent_from_api": len(bc_tx - api_tx),
        "file_fill_rows": len(fills),
        "file_aggregate_rows": sum(r["fill_role"] == "taker_aggregate" for r in fills),
        "file_aggregate_rows_unmatched_by_api": sum(
            r["fill_role"] == "taker_aggregate"
            for bucket in candidates.values()
            for r in bucket
        ),
        "unmatched_examples": failures,
    }


def scan_chain(
    client: niquests.Session, path: Path, day: str, output: Path
) -> dict[str, Any]:
    """Full-day log identity reconciliation against SQD's five deployments.

    Sort only three parquet columns; compare one block at a time. No full-day
    Python set, and no writes to the collector's output directory.
    """
    evidence: dict[str, Any] = {}
    first, last, start, end = resolve_block_range(client, day, evidence=evidence)
    rows = (
        pl.scan_parquet(path)
        .select("block_number", pl.col("tx_hash").str.to_lowercase(), "log_index")
        .sort("block_number", "log_index")
        .collect()
    )
    row_iter = rows.iter_rows()
    pending = next(row_iter, None)
    counts: Counter[str] = Counter()
    absent: Counter[str] = Counter()
    extra = blocks = 0
    examples: list[dict[str, Any]] = []
    sample_tokens: dict[str, str] = {}
    with (
        (output / "chain_missing_examples.ndjson").open("wb") as raw,
        closing(stream_blocks(client, first, last)) as stream,
    ):
        for block in stream:
            number, timestamp = (
                block["header"]["number"],
                block["header"]["timestamp"],
            )
            if not start <= timestamp < end:
                raise ValueError("SQD block outside verified UTC day")
            blocks += 1
            local: set[tuple[str, int]] = set()
            while pending is not None and pending[0] <= number:
                if pending[0] < number:
                    extra += 1
                else:
                    local.add((pending[1], pending[2]))
                pending = next(row_iter, None)
            source: set[tuple[str, int]] = set()
            for entry in block.get("logs", []):
                decoded = decode_order_filled(entry, number, timestamp)
                identity = (decoded["tx_hash"], decoded["log_index"])
                if identity in source:
                    raise ValueError("Duplicate SQD log within a block")
                source.add(identity)
                address = entry["address"].lower()
                counts[address] += 1
                if identity not in local:
                    absent[address] += 1
                    if address not in sample_tokens:
                        sample_tokens[address] = decoded["token_id"]
                    if len(examples) < 20:
                        example = {
                            "exchange": address,
                            "raw_log": entry,
                            "decoded": decoded,
                        }
                        examples.append(example)
                        raw.write(orjson.dumps(example) + b"\n")
            extra += len(local - source)
            if blocks % 4000 == 0:
                print(
                    f"Chain scan: {blocks:,} blocks; {sum(counts.values()):,} logs; {sum(absent.values()):,} absent",
                    flush=True,
                )
    if pending is not None:
        extra += 1 + sum(1 for _ in row_iter)
    if blocks != last - first + 1:
        raise ValueError("Incomplete block scan")
    return {
        **evidence,
        "blocks_scanned": blocks,
        "source_logs_by_exchange": {a: counts[a] for a in EXCHANGES},
        "missing_logs_by_exchange": {a: absent[a] for a in EXCHANGES},
        "local_logs_absent_from_sqd": extra,
        "missing_examples": examples,
        "sample_tokens_from_missing_exchanges": sample_tokens,
        "comparison": "Exact block_number/tx_hash/log_index identities; quantities not compared here",
        "independent_rpc_verified": False,
    }


def audit(args: argparse.Namespace) -> dict[str, Any]:
    start = int(
        datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    end = start + 86400
    frame = canonical_fills(args.parquet)
    summary = (
        frame.select(
            pl.len().alias("rows"),
            pl.struct("tx_hash", "log_index").n_unique().alias("unique_log_identities"),
            pl.col("timestamp").min().alias("first_timestamp"),
            pl.col("timestamp").max().alias("last_timestamp"),
            pl.col("token_id").n_unique().alias("unique_tokens"),
            pl.col("tx_hash").n_unique().alias("unique_transactions"),
            ((pl.col("timestamp") < start) | (pl.col("timestamp") >= end))
            .sum()
            .alias("outside_day_rows"),
        )
        .collect()
        .row(0, named=True)
    )
    args.output.mkdir(parents=True, exist_ok=True)
    with args.parquet.open("rb") as input_file:
        input_digest = hashlib.file_digest(input_file, "sha256").hexdigest()
    report: dict[str, Any] = {
        "date_utc": args.date,
        "run_time_utc": datetime.now(UTC).isoformat(),
        "input": str(args.parquet.resolve()),
        "input_sha256": input_digest,
        "schema": pq.read_schema(args.parquet).names,
        "current_collector_completion_verified": is_complete_file(
            args.parquet, args.date
        ),
        "file_summary": summary,
        "api_scope": "Deterministic token-volume sample plus missing-exchange tokens; not all markets",
        "markets": [],
        "errors": [],
    }
    with niquests.Session() as client:
        report["global_time_filter_probes"] = []
        for endpoint in ("/trades", "/v2/trades"):
            data = get_json(
                client,
                API + endpoint,
                {"start": str(start), "end": str(end - 1), "limit": "2"},
            )
            (
                args.output / f"probe_{endpoint.strip('/').replace('/', '_')}.json"
            ).write_bytes(orjson.dumps(data))
            records = data if isinstance(data, list) else data["data"]
            report["global_time_filter_probes"].append(
                {
                    "endpoint": endpoint,
                    "returned_timestamps": [r["timestamp"] for r in records],
                    "out_of_requested_day": sum(
                        not start <= r["timestamp"] < end for r in records
                    ),
                }
            )
        if args.chain_scan:
            try:
                report["chain_scan"] = scan_chain(
                    client, args.parquet, args.date, args.output
                )
            except (
                ValueError,
                TypeError,
                RuntimeError,
                TimeoutError,
                niquests.exceptions.RequestException,
            ) as exc:
                report["errors"].append(f"Chain scan failed: {exc}")
        counts = frame.group_by("token_id").len().sort("len", "token_id").collect()
        tokens: list[str] = []
        for low, high in ((10, 500), (501, 3000), (3001, 2**32 - 1)):
            tier = counts.filter(pl.col("len").is_between(low, high))
            if tier.height:
                for i in sorted({0, tier.height // 2, tier.height - 1}):
                    tokens.append(tier["token_id"][i])
        if args.date == "2026-07-12":
            tokens.append(BTC_TOKEN)
        tokens.extend(
            report.get("chain_scan", {})
            .get("sample_tokens_from_missing_exchanges", {})
            .values()
        )
        tokens = list(dict.fromkeys(tokens))
        mapping = get_json(client, f"{API}/v2/tokens", {"token_id": ",".join(tokens)})
        (args.output / "token_mapping.json").write_bytes(orjson.dumps(mapping))
        if not isinstance(mapping, dict) or not isinstance(mapping.get("data"), list):
            raise TypeError("Invalid token mapping response")
        mapped = {m["token_id"] for m in mapping["data"]}
        report["unmapped_sample_tokens"] = sorted(set(tokens) - mapped)
        conditions = sorted({m["condition_id"] for m in mapping["data"]})
        for condition in conditions:
            try:
                # Resolve all outcomes, including tokens absent from the local file.
                market = get_json(client, f"{API}/v2/tokens", {"condition": condition})
                token_ids = [m["token_id"] for m in market["data"]]
                trades, pagination = fetch_market(
                    client, condition, start, end, args.output, args.max_pages
                )
                known_tokens = set(token_ids)
                if any(t["token_id"] not in known_tokens for t in trades):
                    raise ValueError("Trade token absent from condition lookup")
                fills = (
                    frame.filter(
                        pl.col("token_id").is_in(token_ids)
                        & pl.col("timestamp").is_between(start, end - 1)
                    )
                    .collect()
                    .to_dicts()
                )
                result = {
                    "condition": condition,
                    "tokens": token_ids,
                    **pagination,
                    **compare_details(fills, trades),
                }
                report["markets"].append(result)
                print(
                    f"API {condition[:12]}: {len(trades):,} day trades; {result['matched_detail_rows']:,} detail matches; {result['unmatched_detail_rows']:,} unmatched; terminal={pagination['terminal_cursor_reached']}",
                    flush=True,
                )
            except (
                KeyError,
                ValueError,
                TypeError,
                RuntimeError,
                niquests.exceptions.RequestException,
            ) as exc:
                report["errors"].append(f"Market {condition}: {exc}")
    scan = report.get("chain_scan")
    discrepancies = (
        summary["rows"] != summary["unique_log_identities"]
        or summary["outside_day_rows"] > 0
    )
    if scan:
        discrepancies |= bool(
            sum(scan["missing_logs_by_exchange"].values())
            or scan["local_logs_absent_from_sqd"]
        )
    discrepancies |= any(
        m["unmatched_detail_rows"]
        or (m["terminal_cursor_reached"] and m["file_aggregate_rows_unmatched_by_api"])
        for m in report["markets"]
    )
    complete_audit = bool(
        scan
        and report["markets"]
        and not report["errors"]
        and not report["unmapped_sample_tokens"]
        and all(m["terminal_cursor_reached"] for m in report["markets"])
    )
    report["verdict"] = (
        "DISCREPANCIES_FOUND"
        if discrepancies
        else "MATCHES_SQD_AND_API_SAMPLE"
        if complete_audit
        else "INCONCLUSIVE"
    )
    report["all_polymarket_trades_proven_complete"] = False
    report["exit_code"] = 1 if discrepancies else 0 if complete_audit else 2
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="2026-07-12")
    parser.add_argument(
        "--parquet",
        type=Path,
        default=ROOT / "dataset/polymarket_orders_2026_07_12.parquet",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "docs/review_2026_07_12")
    parser.add_argument("--chain-scan", action="store_true")
    parser.add_argument("--max-pages", type=int, default=50)
    args = parser.parse_args()
    if args.max_pages < 1:
        parser.error("--max-pages must be positive")
    report = audit(args)
    target = args.output / "report.json"
    target.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    print(f"{report['verdict']}: {target}")
    raise SystemExit(report["exit_code"])


if __name__ == "__main__":
    main()
