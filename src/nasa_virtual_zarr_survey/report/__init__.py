"""Roll up per-collection verdicts across phases and render report.md.

This package is split into four modules so the data path and render path
can be tested independently:

* ``_ingest`` — DuckDB / Parquet attachment helpers
* ``_aggregate`` — verdicts, taxonomy, cubability, ``ThreePhaseRow`` rollups
* ``_markdown`` — pure renderers from aggregated data to Markdown text
* ``_orchestrate`` — ``run_report`` glue: ingest → aggregate → render
"""

from nasa_virtual_zarr_survey.report._aggregate import (
    RunMetadata,
    ThreePhaseRow,
    collection_verdicts,
    three_phase_rows,
)
from nasa_virtual_zarr_survey.report._markdown import (
    render_report,
)
from nasa_virtual_zarr_survey.report._orchestrate import run_report

__all__ = [
    "RunMetadata",
    "ThreePhaseRow",
    "collection_verdicts",
    "render_report",
    "run_report",
    "three_phase_rows",
]
