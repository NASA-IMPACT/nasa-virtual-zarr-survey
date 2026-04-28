"""Glue: ``render`` reads survey state, calls aggregation, calls rendering.

The work is done by ``_aggregate`` and ``_markdown``. This module exists so
the data path and the render path can be tested independently.

Public API: :func:`render` (no path args; reads from / writes to the
canonical paths under cwd). The fuller :func:`_run_render` is what
``snapshot.run`` uses to inject custom paths and a custom locked-sample
state.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

from vzc.render._aggregate import (
    RunMetadata,
    collect_run_metadata,
    collection_verdicts,
    cubability_results,
    other_errors_for_phase,
    skipped_by_format,
    taxonomy_counts,
)
from vzc.render._markdown import render_report

if TYPE_CHECKING:
    from vzc.snapshot import RunInputs
    from vzc.state._io import SurveyState


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def render(*, from_data: Path | str | None = None, history: bool = False) -> None:
    """Read survey state plus Parquet results and write the report.

    Reads ``output/state.json`` and ``output/results/`` (relative to cwd).
    Writes ``docs/results/index.md`` plus figure assets under
    ``docs/results/figures/``. Idempotent and cheap: re-run after refining
    ``taxonomy.py`` to update the Markdown without re-running ``attempt``.

    Parameters
    ----------
    from_data:
        When provided, load verdicts and taxonomy from a committed
        ``*.summary.json`` digest and skip state / Parquet queries entirely.
        Used by CI to regenerate the report without a live survey run.
    history:
        Also re-render the Coverage-over-time page from committed digests
        under ``docs/results/history/``.
    """
    from vzc._config import (
        DEFAULT_HISTORY_DIR,
        DEFAULT_HISTORY_PAGE,
        DEFAULT_INTROS_PATH,
        DEFAULT_REPORT,
        DEFAULT_RESULTS,
        DEFAULT_STATE_PATH,
    )
    from vzc.render._history import run_history
    from vzc.state._io import load_state

    state = None if from_data is not None else load_state(DEFAULT_STATE_PATH)
    _run_render(
        state=state,
        results_dir=DEFAULT_RESULTS,
        out_path=DEFAULT_REPORT,
        from_data=from_data,
    )

    if history:
        run_history(
            DEFAULT_HISTORY_DIR, DEFAULT_HISTORY_PAGE, intros_path=DEFAULT_INTROS_PATH
        )


def _run_render(
    *,
    state: "SurveyState | None",
    results_dir: Path | str,
    out_path: Path | str,
    export_to: Path | str | None = None,
    from_data: Path | str | None = None,
    snapshot: "RunInputs | None" = None,
    no_render: bool = False,
) -> None:
    """Run the render pipeline against an explicit state + paths.

    ``state`` must be ``None`` exactly when ``from_data`` is set. The
    snapshot path injects a state loaded from ``config/locked_sample.json``;
    the public :func:`render` always reads from ``output/state.json``.
    """
    if export_to is not None and from_data is not None:
        raise ValueError("export_to and from_data are mutually exclusive")

    snap_date = snapshot.snapshot_date if snapshot is not None else None
    snap_kind = snapshot.snapshot_kind if snapshot is not None else None
    snap_label = snapshot.label if snapshot is not None else None
    snap_desc = snapshot.description if snapshot is not None else None
    locked_sample_path = snapshot.locked_sample_path if snapshot is not None else None

    out_path = Path(out_path)
    if not no_render:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    from vzc import __version__
    from vzc.render import _figures

    if snap_kind is None and snap_date is not None:
        snap_kind = "preview" if snap_label else "release"

    if from_data is not None:
        from vzc.state._digest import load_summary

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
        if state is None:
            raise ValueError("state is required when from_data is not set")
        verdicts = collection_verdicts(state, results_dir)
        parse_tax = taxonomy_counts(results_dir, "parse")
        dataset_tax = taxonomy_counts(results_dir, "dataset")
        datatree_tax = taxonomy_counts(results_dir, "datatree")
        cube_results = cubability_results(results_dir, verdicts)
        other_parse_errors = other_errors_for_phase(results_dir, "parse")
        other_dataset_errors = other_errors_for_phase(results_dir, "dataset")
        other_datatree_errors = other_errors_for_phase(results_dir, "datatree")
        skipped_format_rows = skipped_by_format(state)
        metadata = collect_run_metadata(state, __version__)

        if export_to is not None:
            from vzc.state._digest import dump_summary

            locked_sha = (
                _sha256_of_file(Path(locked_sample_path))
                if locked_sample_path is not None
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
                snapshot_date=snap_date,
                snapshot_kind=snap_kind,
                label=snap_label,
                description=snap_desc,
                locked_sample_sha256=locked_sha,
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
