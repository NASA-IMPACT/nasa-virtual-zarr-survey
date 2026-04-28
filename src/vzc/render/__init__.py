"""Aggregation, rendering, and history charts.

This package keeps the render path testable without the survey runtime: feed
``render_report`` pre-computed verdicts plus taxonomy counts and it produces
Markdown without touching state.json or Parquet.
"""

from vzc.render._aggregate import (
    RunMetadata,
    ThreePhaseRow,
    collect_run_metadata,
    collection_verdicts,
    cubability_results,
    other_errors_for_phase,
    skipped_by_format,
    taxonomy_counts,
    three_phase_rows,
)
from vzc.render._history import run_history
from vzc.render._markdown import render_report
from vzc.render._orchestrate import render

__all__ = [
    "RunMetadata",
    "ThreePhaseRow",
    "collect_run_metadata",
    "collection_verdicts",
    "cubability_results",
    "other_errors_for_phase",
    "render_report",
    "run_history",
    "render",
    "skipped_by_format",
    "taxonomy_counts",
    "three_phase_rows",
]
