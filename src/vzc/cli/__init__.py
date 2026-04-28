"""CLI helpers and subcommands for the ``vzc`` entry point.

Re-exports path and cache defaults from :mod:`vzc._config` so CLI
subcommands can pull everything from one place.
"""

from __future__ import annotations

import logging
import sys

from vzc._config import (
    AccessMode,
    DEFAULT_CACHE_DIR,
    DEFAULT_CACHE_MAX_BYTES,
    DEFAULT_CACHE_MAX_SIZE,
    DEFAULT_REPORT,
    DEFAULT_RESULTS,
    DEFAULT_STATE_PATH,
)


def configure_logging(verbose: bool) -> None:
    """Set the ``vzc`` logger level for a CLI invocation.

    ``verbose=True`` enables ``INFO`` (per-batch / per-collection /
    per-granule progress); the default ``WARNING`` only surfaces problems.
    Idempotent: subsequent calls overwrite the level on the same handler
    rather than stacking duplicates.
    """
    logger = logging.getLogger("vzc")
    logger.setLevel(logging.INFO if verbose else logging.WARNING)
    if not any(getattr(h, "_vzc_cli", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        handler._vzc_cli = True  # type: ignore[attr-defined]
        logger.addHandler(handler)


__all__ = [
    "AccessMode",
    "DEFAULT_CACHE_DIR",
    "DEFAULT_CACHE_MAX_BYTES",
    "DEFAULT_CACHE_MAX_SIZE",
    "DEFAULT_REPORT",
    "DEFAULT_RESULTS",
    "DEFAULT_STATE_PATH",
    "configure_logging",
]
