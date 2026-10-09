"""Decode supported Polygon CLOB fills into the existing Parquet columns.

Dispatch uses the emitting deployment and signature, not date estimates. V1 and
V2 deployments overlap. V3 uses the V2 event layout but its token_id identifies
a Protocol V2 position. All supported amounts have six decimal places.

fee preserves the native event units: V1 BUY fees are outcome shares, V1 SELL
fees are collateral; V2/V3 fees are collateral. It is not uniformly a USD fee.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, TypedDict

from poly_data_storage.constants import (
    EXCHANGES,
    ORDER_FILLED_TOPIC,
    ORDER_FILLED_V1_TOPIC,
)


class OrderFill(TypedDict):
    block_number: int
    timestamp: int
    block_time: str
    tx_hash: str
    log_index: int
    order_hash: str
    maker: str
    taker: str
    is_taker: bool
    is_sell: bool
    token_id: str
    fee: float
    amount_usd: float
    shares: float
    price: float | None


class DecodeError(ValueError):
    """A matching log could not be decoded without discarding source data."""


def _hex(value: Any, byte_count: int, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 2 + byte_count * 2
        or not value.startswith("0x")
    ):
        raise DecodeError(f"Invalid {name}: expected {byte_count} hex bytes")
    try:
        if len(bytes.fromhex(value[2:])) != byte_count:
            raise ValueError("Incomplete hex bytes")
    except ValueError as exc:
        raise DecodeError(f"Invalid {name}: expected {byte_count} hex bytes") from exc
    return value.lower()


def _address(topic: str) -> str:
    if topic[2:26] != "0" * 24:
        raise DecodeError("Address topic has nonzero ABI padding")
    return "0x" + topic[26:]


def _decode_v1(fields: list[int]) -> tuple[bool, int, int, int, int]:
    maker_asset, taker_asset, maker_amount, taker_amount, fee = fields
    if maker_asset == 0 and taker_asset != 0:
        return False, taker_asset, maker_amount, taker_amount, fee
    if taker_asset == 0 and maker_asset != 0:
        return True, maker_asset, taker_amount, maker_amount, fee
    raise DecodeError("V1 fill must exchange collateral (asset 0) and an outcome")


def _decode_v2(fields: list[int]) -> tuple[bool, int, int, int, int]:
    side, token_id, maker_amount, taker_amount, fee, _builder, _metadata = fields
    if side == 0:
        return False, token_id, maker_amount, taker_amount, fee
    if side == 1:
        return True, token_id, taker_amount, maker_amount, fee
    raise DecodeError(f"Unsupported order side: {side}")


def decode_order_filled(
    raw_log: dict[str, Any], block_num: int, block_ts: int
) -> OrderFill:
    """Decode one fill or raise; never silently drop a matching log."""
    address = _hex(raw_log.get("address"), 20, "exchange address")
    version = EXCHANGES.get(address)
    if version is None:
        raise DecodeError(f"Unsupported exchange: {address}")
    topics = raw_log.get("topics")
    if not isinstance(topics, list) or len(topics) != 4:
        raise DecodeError("OrderFilled requires exactly four topics")
    topics = [_hex(topic, 32, "topic") for topic in topics]
    expected_topic = ORDER_FILLED_V1_TOPIC if version == "v1" else ORDER_FILLED_TOPIC
    if topics[0] != expected_topic:
        raise DecodeError(f"Unexpected OrderFilled signature for {address}")

    word_count = 5 if version == "v1" else 7
    data = _hex(raw_log.get("data"), word_count * 32, "event data")[2:]
    fields = [int(data[i : i + 64], 16) for i in range(0, len(data), 64)]
    is_sell, token_id, collateral, shares, fee = (
        _decode_v1(fields) if version == "v1" else _decode_v2(fields)
    )
    tx_hash = _hex(raw_log.get("transactionHash"), 32, "transaction hash")
    log_index = raw_log.get("logIndex")
    if type(log_index) is not int or not 0 <= log_index <= 2**31 - 1:
        raise DecodeError("Invalid logIndex")
    maker, taker = _address(topics[2]), _address(topics[3])
    return {
        "block_number": block_num,
        "timestamp": block_ts,
        "block_time": datetime.fromtimestamp(block_ts, tz=UTC).isoformat(),
        "tx_hash": tx_hash,
        "log_index": log_index,
        "order_hash": topics[1],
        "maker": maker,
        "taker": taker,
        "is_taker": taker == address,
        "is_sell": is_sell,
        "token_id": str(token_id),
        "fee": fee / 1_000_000,
        "amount_usd": collateral / 1_000_000,
        "shares": shares / 1_000_000,
        "price": collateral / shares if shares else None,
    }
