"""Single source of truth for cross-cutting defaults.

Lives outside ``cli`` so low-level modules (``auth``, ``cache``, etc.) can
import the canonical cap without taking on a dependency on the click layer.
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_CACHE_DIR = Path.home() / ".cache" / "nasa-virtual-zarr-survey"

# The cache cap exists in two forms because click options take a string and
# everything else takes an int. ``DEFAULT_CACHE_MAX_BYTES`` MUST equal
# ``_parse_size(DEFAULT_CACHE_MAX_SIZE)``; ``test_defaults_cache_cap_consistent``
# enforces that.
DEFAULT_CACHE_MAX_SIZE = "50GB"
DEFAULT_CACHE_MAX_BYTES = 50 * 1024**3
