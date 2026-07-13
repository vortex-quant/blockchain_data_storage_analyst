"""Polymarket event resolution ingestion — constants and configuration."""

from __future__ import annotations

from pathlib import Path

# ── Network endpoints ─────────────────────────────────────────────────────────

SQD_URL = "https://portal.sqd.dev/datasets/polygon-mainnet/stream"
SQD_TIMESTAMP_URL = (
    "https://portal.sqd.dev/datasets/polygon-mainnet/timestamps/{ts}/block"
)

# ── UMA CTF Adapter addresses on Polygon ──────────────────────────────────────

UMA_CTF_ADAPTERS = [
    "0x6A9D222616C90FCa5754CD1333cFD9b7FB6a4F74",
    "0x71392E133063CC0D16F40E1F9B60227404Bc03f7",
    "0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49",
    "0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d",
    "0x978cF140321276E3938Fdfbf1aC4Ac8D96977716",
]

# ── Event topic0 hashes (keccak256 of signatures) ─────────────────────────────

QUESTION_INITIALIZED_TOPIC = (
    "0x99c28688a4ada391b73fdc0daa54676bd9b334a759fd1707d1f6287e4e7bcea9"
)
QUESTION_RESOLVED_TOPIC = (
    "0x53d187b857c277aea488429fbf8279a7f8ad377b307150cd22f0376b188d62f2"
)

# ── Timing ────────────────────────────────────────────────────────────────────

SQD_DELAY = 0.55
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0
SQD_MAX_BLOCKS_PER_REQUEST = 10_000

# ── Storage ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data")
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6

# ── Ancillary data parsing ────────────────────────────────────────────────────

ASSET_KEYWORDS: list[tuple[str, str]] = [
    ("bitcoin", "BTC"),
    ("ethereum", "ETH"),
    ("solana", "SOL"),
    ("xrp", "XRP"),
    ("ripple", "XRP"),
    ("dogecoin", "DOGE"),
    ("bnb", "BNB"),
    ("hype", "HYPE"),
]

EVENT_TYPE_KEYWORDS: list[tuple[str, str]] = [
    ("5 min", "5m"),
    ("5m", "5m"),
    ("15 min", "15m"),
    ("15m", "15m"),
    ("1 hour", "1h"),
    ("1h", "1h"),
    ("4 hour", "4h"),
    ("4h", "4h"),
    ("daily", "daily"),
]
