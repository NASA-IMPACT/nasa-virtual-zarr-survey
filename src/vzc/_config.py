"""Single source of truth for cross-cutting paths and defaults.

Lives outside ``cli/`` so low-level modules (state loaders, cache, store
helpers) can import the canonical paths without taking on a dependency on
the click layer. Every path is relative to the current working directory
unless overridden by an environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

# ---------------------------------------------------------------------------
# Workspace paths (relative to cwd; overridden by NASA_VZ_SURVEY_CACHE_DIR for
# the cache only)
# ---------------------------------------------------------------------------

DEFAULT_STATE_PATH = Path("output/state.json")
DEFAULT_RESULTS = Path("output/results")
DEFAULT_REPORT = Path("docs/results/index.md")
DEFAULT_HISTORY_DIR = Path("docs/results/history")
DEFAULT_HISTORY_PAGE = Path("docs/results/history.md")
DEFAULT_INTROS_PATH = Path("config/feature_introductions.toml")
DEFAULT_OVERRIDES_PATH = Path("config/collection_overrides.toml")
DEFAULT_LOCKED_SAMPLE = Path("config/locked_sample.json")
DEFAULT_PYPROJECT = Path("pyproject.toml")


def cache_dir() -> Path:
    """Granule cache directory.

    Honors ``NASA_VZ_SURVEY_CACHE_DIR``; otherwise
    ``~/.cache/nasa-virtual-zarr-survey``.
    """
    env = os.environ.get("NASA_VZ_SURVEY_CACHE_DIR")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "nasa-virtual-zarr-survey"


# Back-compat alias for code that imports the static path. Prefer ``cache_dir()``
# in new code so tests can monkey-patch the env var.
DEFAULT_CACHE_DIR = cache_dir()

# Cache cap exists in two forms because click options take a string and
# everything else takes an int. ``DEFAULT_CACHE_MAX_BYTES`` MUST equal
# ``_parse_size(DEFAULT_CACHE_MAX_SIZE)``; the equivalence is asserted
# in ``test_defaults_cache_cap_consistent``.
DEFAULT_CACHE_MAX_SIZE = "50GB"
DEFAULT_CACHE_MAX_BYTES = 50 * 1024**3


# ---------------------------------------------------------------------------
# Workflow constants
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT_S = 60
DEFAULT_SHARD_SIZE = 500

AccessMode = Literal["direct", "external"]
