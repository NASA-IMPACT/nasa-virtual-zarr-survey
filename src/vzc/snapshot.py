"""Run one survey snapshot against the currently-prepared environment.

A snapshot is one re-run of ``attempt`` + ``render --no-render --export ...``
against ``config/locked_sample.json`` under a date-pinned dependency stack.
Each call writes a ``*.summary.json`` digest under
``docs/results/history/<slug>.summary.json``.

Conveniences:

* ``[tool.uv] exclude-newer`` in ``pyproject.toml`` is the default snapshot
  date.
* A ``label`` marks the run as a preview; otherwise the snapshot is a
  release, identified by date alone.

The reproducibility contract is **package version strings + locked-sample
SHA-256** — captured in the digest. Recreating the exact resolved env tree
is not a supported workflow; if you need byte-exact reproduction, capture
the ``uv.lock`` yourself before running.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from vzc._config import (
    AccessMode,
    DEFAULT_HISTORY_DIR,
    DEFAULT_LOCKED_SAMPLE,
    DEFAULT_PYPROJECT,
    cache_dir as _cache_dir,
)


class SnapshotError(ValueError):
    pass


@dataclass(frozen=True)
class RunInputs:
    """Snapshot-related inputs grouped for the renderer.

    ``snapshot_kind`` is ``"release"`` (date-only slug) or ``"preview"``
    (date + label slug). When ``None``, defaults to ``"release"`` if
    ``snapshot_date`` is set.
    """

    snapshot_date: str | None = None
    snapshot_kind: Literal["release", "preview"] | None = None
    label: str | None = None
    description: str | None = None
    locked_sample_path: Path | None = None


def read_pyproject_exclude_newer(
    pyproject: Path | str = DEFAULT_PYPROJECT,
) -> str | None:
    """Return ``[tool.uv] exclude-newer`` from ``pyproject``, or ``None``.

    Accepts both ISO dates (``"2026-02-15"``) and RFC 3339 timestamps
    (``"2026-02-15T00:00:00Z"``); the date prefix is always returned.
    """
    p = Path(pyproject)
    if not p.exists():
        return None
    data = tomllib.loads(p.read_text())
    raw = data.get("tool", {}).get("uv", {}).get("exclude-newer")
    if not isinstance(raw, str) or not raw:
        return None
    return raw[:10]


def run(
    *,
    snapshot_date: str | None = None,
    label: str | None = None,
    description: str | None = None,
    access: AccessMode = "external",
) -> Path:
    """Run attempt + render against ``config/locked_sample.json`` and write a digest.

    Returns the path to the written ``*.summary.json``. Reads
    ``[tool.uv] exclude-newer`` from ``pyproject.toml`` for the default
    date. With ``access="external"`` (the default) the operator must have
    populated the cache via ``prefetch`` first; missing granules in attempt
    will fail fast.
    """
    return _run(
        snapshot_date=snapshot_date,
        label=label,
        description=description,
        access=access,
    )


def _run(
    *,
    snapshot_date: str | None = None,
    label: str | None = None,
    description: str | None = None,
    access: AccessMode = "external",
    locked_sample_path: Path | str = DEFAULT_LOCKED_SAMPLE,
    history_dir: Path | str = DEFAULT_HISTORY_DIR,
    pyproject_path: Path | str = DEFAULT_PYPROJECT,
    results_dir: Path | str | None = None,
    cache_dir: Path | None = None,
) -> Path:
    """Inner implementation; takes explicit paths for tests + alternate workflows."""
    from vzc.pipeline._attempt import _run_attempt
    from vzc.render._orchestrate import _run_render
    from vzc.state._io import load_state

    history_dir = Path(history_dir)
    locked_sample_path = Path(locked_sample_path)

    effective_date = snapshot_date or read_pyproject_exclude_newer(pyproject_path)
    if effective_date is None:
        raise SnapshotError(
            "No snapshot_date provided and [tool.uv] exclude-newer is not "
            "set in pyproject.toml. Either pass snapshot_date or set "
            "[tool.uv] exclude-newer."
        )

    if label:
        snapshot_kind: Literal["release", "preview"] = "preview"
        slug = f"{effective_date}-{label}"
    else:
        snapshot_kind = "release"
        slug = effective_date
        if description is not None:
            raise SnapshotError(
                "--description requires --label (only previews carry "
                "a human-readable description)."
            )

    if results_dir is None:
        results_dir = Path(f"output/snapshots/{slug}/results")
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)

    if cache_dir is None and access == "external":
        cache_dir = _cache_dir()

    history_summary = history_dir / f"{slug}.summary.json"
    state = load_state(locked_sample_path)

    _run_attempt(
        state,
        access=access,
        timeout_s=60,
        results_dir=results_dir,
        cache_dir=cache_dir,
        skip_override_validation=True,
    )
    _run_render(
        state=state,
        results_dir=results_dir,
        out_path=Path("docs/results/index.md"),
        export_to=history_summary,
        snapshot=RunInputs(
            snapshot_date=effective_date,
            snapshot_kind=snapshot_kind,
            label=label,
            description=description,
            locked_sample_path=locked_sample_path,
        ),
        no_render=True,
    )
    return history_summary
