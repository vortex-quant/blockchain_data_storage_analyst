"""Resolve proxy → aggregator addresses via RPC storage reads.

The EACAggregatorProxy's aggregator() function is access-controlled and
reverts for unauthorised callers. Instead, we read the currentPhase struct
directly from storage slot 2:

    struct Phase { uint16 id; address aggregator; }

Solidity packs this struct into a single 32-byte slot:
  - bytes 30-31 (rightmost 2): phase id (uint16)
  - bytes 10-29 (next 20):      aggregator address
"""

from __future__ import annotations

import niquests

from chainlink_asset_storage.constants import (
    PHASE_STORAGE_SLOT,
    PROXY_ADDRESSES,
    RPC_URL,
)
from chainlink_asset_storage.logger import get_logger

log = get_logger()


def _read_storage_slot(
    client: niquests.Session,
    address: str,
    slot: int,
) -> str:
    """Read a raw 32-byte storage slot from a contract via JSON-RPC."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getStorageAt",
        "params": [address, hex(slot), "latest"],
        "id": 1,
    }
    resp = client.post(RPC_URL, json=payload, timeout=30.0)
    resp.raise_for_status()
    result = resp.json()
    if "result" not in result or result["result"] == "0x":
        raise ValueError(f"Empty storage at slot {slot} for {address}")
    return result["result"]


def _decode_phase(raw: str) -> tuple[int, str]:
    """Decode a Phase struct from a 32-byte hex value.

    Returns (phase_id, aggregator_address).
    """
    hex_val = raw[2:] if raw.startswith("0x") else raw
    phase_id = int(hex_val[-4:], 16)
    aggregator = "0x" + hex_val[20:60]
    return phase_id, aggregator


def resolve_aggregators(client: niquests.Session) -> dict[str, str]:
    """Resolve all proxy → aggregator mappings via storage reads.

    Returns {asset_symbol: aggregator_address}.
    """
    aggregators: dict[str, str] = {}
    for asset, proxy in PROXY_ADDRESSES.items():
        raw = _read_storage_slot(client, proxy, PHASE_STORAGE_SLOT)
        phase_id, agg = _decode_phase(raw)
        aggregators[asset] = agg
        log.info(f"  {asset}: proxy={proxy} aggregator={agg} (phase {phase_id})")
    return aggregators
