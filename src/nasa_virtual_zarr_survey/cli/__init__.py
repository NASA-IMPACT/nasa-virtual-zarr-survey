"""CLI helpers and subcommands for ``nasa-virtual-zarr-survey``.

The package layout splits the previously-monolithic ``__main__.py`` into
focused modules so individual commands can be edited without loading the
world. ``__main__.py`` builds the click group and registers each command
module via its ``register(group)`` entry point.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

# Re-export the shared cache defaults so cli subcommands can pull everything
# from one place. The canonical definition lives in
# ``nasa_virtual_zarr_survey._defaults`` so non-cli modules (``auth``, etc.)
# can import the same values without depending on click.
from nasa_virtual_zarr_survey._defaults import (
    DEFAULT_CACHE_DIR,
    DEFAULT_CACHE_MAX_BYTES,
    DEFAULT_CACHE_MAX_SIZE,
)

AccessMode = Literal["direct", "external"]

DEFAULT_DB = Path("output/survey.duckdb")
DEFAULT_RESULTS = Path("output/results")
DEFAULT_REPORT = Path("docs/results/index.md")

__all__ = [
    "AccessMode",
    "DEFAULT_DB",
    "DEFAULT_RESULTS",
    "DEFAULT_REPORT",
    "DEFAULT_CACHE_DIR",
    "DEFAULT_CACHE_MAX_BYTES",
    "DEFAULT_CACHE_MAX_SIZE",
]
