"""virtual-zarr-coverage: measure VirtualiZarr coverage of cloud-hosted NASA CMR collections.

Public API. Phase functions take no path arguments — every path is
hardcoded against the current working directory (``output/state.json``,
``output/results/``, ``docs/results/index.md``, ``config/...``). For tests
that need an isolated workspace, use ``monkeypatch.chdir(tmp_path)``.

The cache directory honors ``NASA_VZ_SURVEY_CACHE_DIR``; default is
``~/.cache/nasa-virtual-zarr-survey``.
"""

try:
    from importlib.metadata import version

    __version__ = version("virtual-zarr-coverage")
except Exception:
    __version__ = "0.1.0"


from vzc.cmr._discover import discover
from vzc.cmr._sample import sample
from vzc.pipeline import (
    AttemptResult,
    CollectionOverride,
    OverrideRegistry,
)
from vzc.pipeline._attempt import attempt
from vzc.pipeline._investigate import investigate
from vzc.pipeline._prefetch import prefetch
from vzc.render._orchestrate import render
from vzc.snapshot import RunInputs, run

__all__ = [
    "AttemptResult",
    "CollectionOverride",
    "OverrideRegistry",
    "RunInputs",
    "attempt",
    "discover",
    "investigate",
    "prefetch",
    "render",
    "run",
    "sample",
]
