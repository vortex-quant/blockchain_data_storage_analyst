"""Simple logger — mirrors print output to console and a per-run log file."""

from __future__ import annotations

import logging
from pathlib import Path

_LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
_logger: logging.Logger | None = None


def _next_log_file() -> Path:
    """Find the next available chainlink_fetch_<n>.log filename."""
    n = 1
    while True:
        candidate = _LOG_DIR / f"chainlink_fetch_{n}.log"
        if not candidate.exists():
            return candidate
        n += 1


def get_logger() -> logging.Logger:
    """Return a logger that writes to both stdout and a per-run log file."""
    global _logger
    if _logger is not None:
        return _logger

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = _next_log_file()

    logger = logging.getLogger("chainlink_fetch")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter("%(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    _logger = logger
    return logger
