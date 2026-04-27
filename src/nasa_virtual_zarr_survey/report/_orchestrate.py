"""Glue: ``run_report`` reads survey state, calls aggregation, calls rendering.

Holds nothing interesting on its own — the work is done by ``_ingest``,
``_aggregate``, and ``_markdown``. This module exists so the data path and
the render path can be tested independently.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.report._aggregate import (
    RunMetadata,
    collect_run_metadata,
    collection_verdicts,
    cubability_results,
    other_errors_for_phase,
    skipped_by_format,
    taxonomy_counts,
)
from nasa_virtual_zarr_survey.report._ingest import (
    attach_results,
    register_cached_granules,
)
from nasa_virtual_zarr_survey.report._markdown import render_report

if TYPE_CHECKING:
    from nasa_virtual_zarr_survey.db_session import SurveySession
    from nasa_virtual_zarr_survey.snapshot import SnapshotInputs


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_report(
    session: "SurveySession | Path | str | None",
    results_dir: Path | str,
    out_path: Path | str = "docs/results/index.md",
    *,
    export_to: Path | str | None = None,
    from_data: Path | str | None = None,
    snapshot: "SnapshotInputs | None" = None,
    no_render: bool = False,
    cache_dir: Path | str | None = None,
    cache_only: bool = False,
) -> None:
    """Read DuckDB state plus Parquet results, compute verdicts, and write the report.

    Idempotent and cheap: re-run after refining ``taxonomy.py`` to update the
    Markdown output without re-running ``attempt``.

    Parameters
    ----------
    session:
        A :class:`SurveySession` (or a DuckDB path for legacy callers/tests).
        ``None`` is required when ``from_data`` is set.
    results_dir:
        Directory containing Parquet result shards (ignored when
        ``from_data`` is set).
    out_path:
        Destination Markdown file for the rendered report.
    export_to:
        When provided, serialize all computed data to a compact JSON digest at
        this path. Mutually exclusive with ``from_data``.
    from_data:
        When provided, load verdicts and taxonomy from the given JSON digest
        and skip DuckDB/Parquet queries entirely.
    snapshot:
        Bundle of snapshot-related inputs (date, kind, label, description,
        git_overrides, locked_sample_path, uv_lock_path, preview_manifest_path).
        See :class:`~nasa_virtual_zarr_survey.snapshot.SnapshotInputs`. Only
        consumed when ``export_to`` is set.
    no_render:
        Skip writing the Markdown + figures output.
    cache_dir, cache_only:
        Scope to results whose granule URL is on disk under ``cache_dir``.
        ``cache_only=True`` requires ``cache_dir`` and is mutually exclusive
        with ``from_data``.
    """
    from nasa_virtual_zarr_survey import figures as _figures
    from nasa_virtual_zarr_survey.db_session import SurveySession

    if export_to is not None and from_data is not None:
        raise ValueError("export_to and from_data are mutually exclusive")
    if cache_only and from_data is not None:
        raise ValueError("cache_only and from_data are mutually exclusive")
    if cache_only and cache_dir is None:
        raise ValueError("cache_only=True requires cache_dir to be set")

    snap_date = snapshot.snapshot_date if snapshot is not None else None
    snap_kind = snapshot.snapshot_kind if snapshot is not None else None
    snap_label = snapshot.label if snapshot is not None else None
    snap_desc = snapshot.description if snapshot is not None else None
    snap_overrides = snapshot.git_overrides if snapshot is not None else None
    locked_sample_path = snapshot.locked_sample_path if snapshot is not None else None
    uv_lock_path = snapshot.uv_lock_path if snapshot is not None else None
    preview_manifest_path = (
        snapshot.preview_manifest_path if snapshot is not None else None
    )

    if uv_lock_path is not None and preview_manifest_path is not None:
        raise ValueError(
            "uv_lock_path and preview_manifest_path are mutually exclusive"
        )

    out_path = Path(out_path)
    if not no_render:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    from nasa_virtual_zarr_survey import __version__

    effective_snapshot_date = snap_date

    if preview_manifest_path is not None:
        from nasa_virtual_zarr_survey.preview_manifest import load_manifest

        m = load_manifest(preview_manifest_path)
        snap_kind = "preview"
        snap_label = m.label
        snap_desc = m.description or None
        snap_overrides = m.git_overrides
        effective_snapshot_date = m.snapshot_date
    elif snap_kind is None and snap_date is not None:
        snap_kind = "release"

    if from_data is not None:
        from nasa_virtual_zarr_survey.summary_io import load_summary

        summary = load_summary(from_data)
        verdicts = summary.verdicts
        parse_tax = summary.parse_taxonomy
        dataset_tax = summary.dataset_taxonomy
        datatree_tax = summary.datatree_taxonomy
        cube_results = summary.cubability_results
        other_parse_errors = summary.other_parse_errors
        other_dataset_errors = summary.other_dataset_errors
        other_datatree_errors = summary.other_datatree_errors
        skipped_format_rows = summary.skipped_by_format
        metadata = RunMetadata(
            generated_at=summary.generated_at,
            survey_tool_version=summary.survey_tool_version,
            virtualizarr_version=summary.virtualizarr_version,
            zarr_version=summary.zarr_version,
            xarray_version=summary.xarray_version,
            sampling_mode=summary.sampling_mode,
        )
    else:
        if session is None:
            raise ValueError("session is required when from_data is not set")
        if isinstance(session, SurveySession):
            con = session.con
            effective_session: SurveySession | Path | str = session
        else:
            con = connect(session)
            init_schema(con)
            effective_session = SurveySession(con)
        cache_filter_table: str | None = None
        if cache_only:
            assert cache_dir is not None
            cache_filter_table = register_cached_granules(con, Path(cache_dir))
        attach_results(con, Path(results_dir), cache_filter_table=cache_filter_table)
        verdicts = collection_verdicts(
            effective_session, results_dir, cache_filter_table=cache_filter_table
        )
        parse_tax = taxonomy_counts(con, "parse")
        dataset_tax = taxonomy_counts(con, "dataset")
        datatree_tax = taxonomy_counts(con, "datatree")
        cube_results = cubability_results(con, verdicts)
        other_parse_errors = other_errors_for_phase(con, "parse")
        other_dataset_errors = other_errors_for_phase(con, "dataset")
        other_datatree_errors = other_errors_for_phase(con, "datatree")
        skipped_format_rows = skipped_by_format(con)
        metadata = collect_run_metadata(con, __version__)

        if export_to is not None:
            from nasa_virtual_zarr_survey.summary_io import dump_summary

            locked_sha = (
                _sha256_of_file(Path(locked_sample_path))
                if locked_sample_path is not None
                else None
            )
            uv_lock_sha = (
                _sha256_of_file(Path(uv_lock_path))
                if uv_lock_path is not None
                else None
            )
            dump_summary(
                export_to,
                verdicts=verdicts,
                parse_taxonomy=parse_tax,
                dataset_taxonomy=dataset_tax,
                datatree_taxonomy=datatree_tax,
                cubability_results=cube_results,
                other_parse_errors=other_parse_errors,
                other_dataset_errors=other_dataset_errors,
                other_datatree_errors=other_datatree_errors,
                skipped_by_format=skipped_format_rows,
                survey_tool_version=metadata.survey_tool_version,
                virtualizarr_version=metadata.virtualizarr_version,
                zarr_version=metadata.zarr_version,
                xarray_version=metadata.xarray_version,
                sampling_mode=metadata.sampling_mode,
                generated_at=metadata.generated_at,
                snapshot_date=effective_snapshot_date,
                snapshot_kind=snap_kind,
                label=snap_label,
                description=snap_desc,
                git_overrides=snap_overrides,
                locked_sample_sha256=locked_sha,
                uv_lock_sha256=uv_lock_sha,
            )

    if no_render:
        return

    figure_stems = _figures.generate_all(
        verdicts=verdicts,
        cube_results=cube_results,
        parse_tax=parse_tax,
        dataset_tax=dataset_tax,
        datatree_tax=datatree_tax,
        out_dir=out_path.parent / "figures",
    )
    text = render_report(
        verdicts,
        parse_tax,
        dataset_tax,
        cube_results,
        other_parse_errors,
        other_dataset_errors,
        figure_stems,
        datatree_tax=datatree_tax,
        other_datatree_errors=other_datatree_errors,
        metadata=metadata,
        skipped_by_format=skipped_format_rows,
    )
    out_path.write_text(text)
