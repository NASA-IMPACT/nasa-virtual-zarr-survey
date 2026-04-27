"""``snapshot`` subcommand: attempt + report + provenance digest."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import click

from nasa_virtual_zarr_survey.cli import AccessMode
from nasa_virtual_zarr_survey.cli._options import (
    _cache_options_with_only,
    _max_granule_size_option,
    _parse_size,
    _resolve_cache_params,
)


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--snapshot-date",
        "snapshot_date",
        type=str,
        default=None,
        help="ISO date for the snapshot (e.g. 2026-02-15). Defaults to "
        "[tool.uv] exclude-newer in pyproject.toml.",
    )
    @click.option(
        "--label",
        type=str,
        default=None,
        help="Required when [tool.uv.sources] has git overrides; names the "
        "preview snapshot's output file.",
    )
    @click.option(
        "--description",
        type=str,
        default=None,
        help="Optional one-line description for previews.",
    )
    @click.option(
        "--preview-manifest",
        "preview_manifest_path",
        type=click.Path(path_type=Path),
        default=None,
        help="Use a pre-curated config/snapshot_previews/*.toml manifest instead "
        "of reading pyproject.toml.",
    )
    @click.option(
        "--locked-sample",
        "locked_sample_path",
        type=click.Path(path_type=Path),
        default=Path("config/locked_sample.json"),
    )
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="external",
    )
    @click.option(
        "--uv-lock",
        "uv_lock_path",
        type=click.Path(path_type=Path),
        default=Path("uv.lock"),
        help="Path to the active uv.lock; copied beside the digest for releases.",
    )
    @click.option(
        "--results",
        "results_dir",
        type=click.Path(path_type=Path),
        default=None,
        help="Per-snapshot results directory. Defaults to output/snapshots/<slug>/results.",
    )
    @click.option(
        "--history-dir",
        type=click.Path(path_type=Path),
        default=Path("docs/results/history"),
    )
    @_cache_options_with_only(default_use_cache=True)
    @_max_granule_size_option
    def snapshot(
        snapshot_date: str | None,
        label: str | None,
        description: str | None,
        preview_manifest_path: Path | None,
        locked_sample_path: Path,
        access: str,
        uv_lock_path: Path,
        results_dir: Path | None,
        history_dir: Path,
        use_cache: bool,
        cache_dir: Path | None,
        cache_max_size: str,
        max_granule_size: str | None,
        cache_only: bool,
    ) -> None:
        """Run attempt + report and emit a `*.summary.json` digest.

        Reads ``[tool.uv] exclude-newer`` for the snapshot date and
        ``[tool.uv.sources]`` for git overrides — the same pyproject.toml that
        pinned the env. Pass ``--preview-manifest`` to bypass pyproject.toml
        detection in favor of a pre-curated manifest file.

        Caching is on by default: subsequent snapshots against the same locked
        sample reuse fetched granule bytes. Pass ``--no-cache`` to disable.
        """
        from nasa_virtual_zarr_survey.snapshot import SnapshotError, run_snapshot

        effective_cache_dir, cache_max_bytes = _resolve_cache_params(
            use_cache, cache_dir, cache_max_size
        )
        max_granule_bytes = _parse_size(max_granule_size) if max_granule_size else None
        # _cache_options_with_only already enforces "--cache-only requires --cache".
        try:
            out = run_snapshot(
                snapshot_date=snapshot_date,
                label=label,
                description=description,
                preview_manifest_path=preview_manifest_path,
                locked_sample_path=locked_sample_path,
                access=cast(AccessMode, access),
                uv_lock_path=uv_lock_path,
                results_dir=results_dir,
                history_dir=history_dir,
                cache_dir=effective_cache_dir,
                cache_max_bytes=cache_max_bytes,
                max_granule_bytes=max_granule_bytes,
                cache_only=cache_only,
            )
        except SnapshotError as e:
            raise click.ClickException(str(e)) from e
        click.echo(f"wrote {out}")
