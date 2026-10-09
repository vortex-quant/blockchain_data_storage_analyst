"""Simple logger — mirrors print output to console and a per-run log file."""

from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

_LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"


def get_logger() -> logging.Logger:
    """Return the shared logger without filesystem effects on import."""
    return logging.getLogger("poly_fetch")


def configure_logger() -> None:
    """Configure console and per-run file logging when the CLI starts."""
    logger = get_logger()
    if logger.handlers:
        return

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = _LOG_DIR / f"poly_fetch_{uuid4().hex}.log"

    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
