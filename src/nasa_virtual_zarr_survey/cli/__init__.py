"""CLI helpers and subcommands for ``nasa-virtual-zarr-survey``.

The package layout splits the previously-monolithic ``__main__.py`` into
focused modules so individual commands can be edited without loading the
world. ``__main__.py`` builds the click group and registers each command
module via its ``register(group)`` entry point.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

AccessMode = Literal["direct", "external"]

DEFAULT_DB = Path("output/survey.duckdb")
DEFAULT_RESULTS = Path("output/results")
DEFAULT_REPORT = Path("docs/results/index.md")
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "nasa-virtual-zarr-survey"
DEFAULT_CACHE_MAX_BYTES = 50 * 1024**3

__all__ = [
    "AccessMode",
    "DEFAULT_DB",
    "DEFAULT_RESULTS",
    "DEFAULT_REPORT",
    "DEFAULT_CACHE_DIR",
    "DEFAULT_CACHE_MAX_BYTES",
]
