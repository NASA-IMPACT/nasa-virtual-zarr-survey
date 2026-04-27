"""``report`` subcommand: phase 5 + render."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import click

from nasa_virtual_zarr_survey.cli import (
    DEFAULT_DB,
    DEFAULT_REPORT,
    DEFAULT_RESULTS,
    AccessMode,
)
from nasa_virtual_zarr_survey.cli._options import (
    _cache_options_with_only,
    _resolve_cache_params,
)


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--locked-sample",
        "locked_sample_path",
        type=click.Path(path_type=Path),
        default=None,
        help="Source the session from a locked sample JSON instead of --db. "
        "Mutually exclusive with --from-data.",
    )
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="direct",
        help="Access mode used when constructing a session from --locked-sample.",
    )
    @click.option(
        "--results",
        "results_dir",
        type=click.Path(path_type=Path),
        default=DEFAULT_RESULTS,
    )
    @click.option(
        "--out", "out_path", type=click.Path(path_type=Path), default=DEFAULT_REPORT
    )
    @click.option(
        "--export",
        "export_to",
        type=click.Path(path_type=Path),
        default=None,
        help="Also write a compact JSON digest suitable for regenerating the report in CI.",
    )
    @click.option(
        "--from-data",
        "from_data",
        type=click.Path(path_type=Path),
        default=None,
        help="Regenerate the report from a JSON digest; skip DuckDB/Parquet queries.",
    )
    @click.option(
        "--snapshot-date",
        "snapshot_date",
        type=str,
        default=None,
        help="ISO date for the snapshot (e.g., 2026-02-15). Drives "
        "snapshot_date / snapshot_kind in the exported summary.",
    )
    @click.option(
        "--uv-lock",
        "uv_lock_path",
        type=click.Path(path_type=Path),
        default=None,
        help="Path to a uv.lock file to hash and reference as uv_lock_sha256.",
    )
    @click.option(
        "--preview-manifest",
        "preview_manifest_path",
        type=click.Path(path_type=Path),
        default=None,
        help="Path to a config/snapshot_previews/*.toml. When set, snapshot_kind "
        "becomes 'preview' and label/description/git_overrides come from the "
        "manifest. Mutually exclusive with --uv-lock.",
    )
    @click.option(
        "--no-render",
        "no_render",
        is_flag=True,
        default=False,
        help="Skip the markdown + figures output. Useful when only the "
        "--export JSON digest is wanted.",
    )
    @_cache_options_with_only(default_use_cache=True)
    def report(
        db_path: Path,
        locked_sample_path: Path | None,
        access: str,
        results_dir: Path,
        out_path: Path,
        export_to: Path | None,
        from_data: Path | None,
        snapshot_date: str | None,
        uv_lock_path: Path | None,
        preview_manifest_path: Path | None,
        no_render: bool,
        use_cache: bool,
        cache_dir: Path | None,
        cache_max_size: str,
        cache_only: bool,
    ) -> None:
        """Phase 5 + render: generate the report from survey state OR a committed JSON digest."""
        from nasa_virtual_zarr_survey.db_session import SurveySession
        from nasa_virtual_zarr_survey.report import run_report
        from nasa_virtual_zarr_survey.snapshot import SnapshotInputs

        if export_to is not None and from_data is not None:
            raise click.UsageError("--export and --from-data are mutually exclusive")
        if locked_sample_path is not None and from_data is not None:
            raise click.UsageError(
                "--locked-sample and --from-data are mutually exclusive"
            )
        if uv_lock_path is not None and preview_manifest_path is not None:
            raise click.UsageError(
                "--uv-lock and --preview-manifest are mutually exclusive"
            )
        if cache_only and from_data is not None:
            raise click.UsageError(
                "--cache-only and --from-data are mutually exclusive"
            )

        effective_cache_dir, _ = _resolve_cache_params(
            use_cache, cache_dir, cache_max_size
        )
        # _cache_options_with_only already enforces "--cache-only requires --cache".

        session: SurveySession | None
        if from_data is not None:
            session = None
        elif locked_sample_path is not None:
            session = SurveySession.from_locked_sample(
                locked_sample_path, access=cast(AccessMode, access)
            )
        else:
            session = SurveySession.from_duckdb(db_path)

        snapshot_inputs: SnapshotInputs | None = None
        if (
            snapshot_date is not None
            or locked_sample_path is not None
            or uv_lock_path is not None
            or preview_manifest_path is not None
        ):
            snapshot_inputs = SnapshotInputs(
                snapshot_date=snapshot_date,
                locked_sample_path=locked_sample_path,
                uv_lock_path=uv_lock_path,
                preview_manifest_path=preview_manifest_path,
            )

        run_report(
            session,
            results_dir=results_dir,
            out_path=out_path,
            export_to=export_to,
            from_data=from_data,
            snapshot=snapshot_inputs,
            no_render=no_render,
            cache_dir=effective_cache_dir,
            cache_only=cache_only,
        )
        if export_to:
            click.echo(f"Wrote digest to {export_to}")
        if not no_render:
            click.echo(f"Wrote {out_path}")
