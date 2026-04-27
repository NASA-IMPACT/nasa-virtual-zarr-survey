"""Run one survey snapshot against the currently-prepared environment.

A snapshot bundles `attempt` + `report --no-render --export ...` plus the
bookkeeping the existing CLI commands can't do alone (slug computation,
output paths, copying the active ``uv.lock`` into the history dir). It does
NOT pin the environment — that is the caller's job.

Conveniences:

* ``[tool.uv] exclude-newer`` in pyproject.toml is the default snapshot date.
* Any git-sourced entries in ``[tool.uv.sources]`` mark the run as a preview;
  the exact (url, rev) pairs become the summary's ``git_overrides``.

The two together mean a typical preview workflow is just: edit pyproject.toml,
``uv lock``, then ``nasa-virtual-zarr-survey snapshot --label something``.
"""

from __future__ import annotations

import shutil
import tomllib
from pathlib import Path
from typing import Literal

from nasa_virtual_zarr_survey.attempt import run_attempt
from nasa_virtual_zarr_survey.db_session import SurveySession
from nasa_virtual_zarr_survey.report import run_report

AccessMode = Literal["direct", "external"]


class SnapshotError(ValueError):
    pass


def read_pyproject_exclude_newer(
    pyproject: Path | str = "pyproject.toml",
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


def read_pyproject_git_sources(
    pyproject: Path | str = "pyproject.toml",
) -> dict[str, dict[str, str]]:
    """Return ``{name: {url, rev}}`` for git-sourced uv sources, else ``{}``.

    Reads ``[tool.uv.sources]``. Sources without a ``git`` URL are skipped.
    Refs other than ``rev`` (e.g. ``branch``, ``tag``) are rejected — preview
    snapshots require a hex SHA for reproducibility.
    """
    p = Path(pyproject)
    if not p.exists():
        return {}
    data = tomllib.loads(p.read_text())
    sources = data.get("tool", {}).get("uv", {}).get("sources", {})
    if not isinstance(sources, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for name, entry in sources.items():
        if not isinstance(entry, dict):
            continue
        url = entry.get("git")
        if not url:
            continue
        rev = entry.get("rev")
        if not rev:
            forbidden = sorted(k for k in entry if k in ("branch", "tag"))
            kinds = ", ".join(forbidden) if forbidden else "no rev"
            raise SnapshotError(
                f"[tool.uv.sources].{name} uses {kinds}; previews require a "
                f'hex SHA via `rev = "..."` for reproducibility.'
            )
        out[name] = {"url": url, "rev": rev}
    return out


def run_snapshot(
    *,
    snapshot_date: str | None = None,
    label: str | None = None,
    description: str | None = None,
    preview_manifest_path: Path | str | None = None,
    locked_sample_path: Path | str = Path("config/locked_sample.json"),
    access: AccessMode = "external",
    uv_lock_path: Path | str = Path("uv.lock"),
    results_dir: Path | str | None = None,
    history_dir: Path | str = Path("docs/results/history"),
    pyproject_path: Path | str = "pyproject.toml",
    skip_override_validation: bool = True,
    cache_dir: Path | None = None,
    cache_max_bytes: int = 50 * 1024**3,
    max_granule_bytes: int | None = None,
    cache_only: bool = False,
) -> Path:
    """Run attempt + report and write the snapshot's summary digest.

    Returns the path to the written ``*.summary.json``.

    The snapshot kind is determined automatically:

    * ``preview_manifest_path`` set → preview (manifest fields take precedence).
    * pyproject ``[tool.uv.sources]`` has git entries → preview (a ``label`` is
      required to name the output file; ``description`` is optional).
    * Otherwise → release; the active ``uv.lock`` is copied beside the digest.

    Pass a ``cache_dir`` to reuse fetched granule bytes across snapshots —
    the locked sample is fixed, so the cache is the same across every run.
    """
    history_dir = Path(history_dir)
    locked_sample_path = Path(locked_sample_path)
    uv_lock_path = Path(uv_lock_path)

    if preview_manifest_path is not None:
        from nasa_virtual_zarr_survey.preview_manifest import load_manifest

        m = load_manifest(preview_manifest_path)
        slug = f"{m.snapshot_date}-{m.label}"
        effective_date: str | None = m.snapshot_date
        snapshot_kind: str | None = "preview"
        effective_label: str | None = m.label
        effective_description: str | None = m.description or None
        effective_overrides: dict[str, dict[str, str]] | None = m.git_overrides
    else:
        effective_date = snapshot_date or read_pyproject_exclude_newer(pyproject_path)
        if effective_date is None:
            raise SnapshotError(
                "No snapshot_date provided and [tool.uv] exclude-newer is not "
                "set in pyproject.toml. Either pass snapshot_date or set "
                "[tool.uv] exclude-newer."
            )
        git_overrides = read_pyproject_git_sources(pyproject_path)
        if git_overrides:
            if not label:
                raise SnapshotError(
                    "Detected git-sourced entries in [tool.uv.sources]; this "
                    "run is a preview and requires a `label` to name the "
                    "output file."
                )
            snapshot_kind = "preview"
            effective_label = label
            effective_description = description
            effective_overrides = git_overrides
            slug = f"{effective_date}-{label}"
        else:
            snapshot_kind = "release"
            effective_label = None
            effective_description = None
            effective_overrides = None
            slug = effective_date

    if results_dir is None:
        results_dir = Path(f"output/snapshots/{slug}/results")
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)

    history_summary = history_dir / f"{slug}.summary.json"

    # Release snapshots: copy the active uv.lock into the history dir so the
    # exact resolved env can be reproduced later. Previews skip uv.lock.
    history_lock: Path | None = None
    if snapshot_kind == "release" and uv_lock_path.exists():
        history_lock = history_dir / f"{slug}.uv.lock"
        shutil.copyfile(uv_lock_path, history_lock)

    session = SurveySession.from_locked_sample(locked_sample_path, access=access)
    run_attempt(
        session,
        results_dir,
        access=access,
        skip_override_validation=skip_override_validation,
        cache_dir=cache_dir,
        cache_max_bytes=cache_max_bytes,
        max_granule_bytes=max_granule_bytes,
        cache_only=cache_only,
    )
    run_report(
        session,
        results_dir,
        export_to=history_summary,
        snapshot_date=effective_date,
        snapshot_kind=snapshot_kind,
        label=effective_label,
        description=effective_description,
        git_overrides=effective_overrides,
        locked_sample_path=locked_sample_path,
        uv_lock_path=history_lock,
        no_render=True,
    )
    return history_summary
