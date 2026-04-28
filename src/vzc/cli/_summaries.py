"""One-line summaries printed at the end of CLI subcommands.

Each function reads ``state.json`` and ``output/results/*.parquet`` directly
(no DuckDB), producing the same human-readable lines the previous DB-backed
versions did.
"""

from __future__ import annotations

from pathlib import Path

from vzc.state._results import load_table
from vzc.state._io import load_state


def _discover_summary(state_path: Path) -> str:
    state = load_state(state_path)
    total = len(state.collections)
    skipped = sum(1 for c in state.collections if c.skip_reason is not None)
    array_like = total - skipped
    return (
        f"discover: {total} collections "
        f"({array_like} array-like, {skipped} skipped as non-array format)"
    )


def _sample_summary(state_path: Path) -> str:
    state = load_state(state_path)
    n_gran = len(state.granules)
    n_coll = len({g.collection_concept_id for g in state.granules})
    return f"sample: {n_gran} granules across {n_coll} collections"


def _attempt_summary(state_path: Path, results_dir: Path, this_run: int) -> str:
    state = load_state(state_path)
    total_granules = len(state.granules)

    if total_granules == 0:
        return (
            "attempt: 0 new attempts (the granules table is empty; "
            "run 'sample' or 'discover' first)"
        )

    table = load_table(
        results_dir,
        columns=[
            "collection_concept_id",
            "parse_success",
            "dataset_success",
            "success",
        ],
    )
    if table is None:
        if this_run == 0:
            return (
                f"attempt: 0 new attempts "
                f"({total_granules} granules pending, 0 results written; "
                "if you expected attempts to happen, check the logs above)"
            )
        return (
            f"attempt: {this_run} new attempts "
            f"(0 of {total_granules} total granules complete; no results written yet)"
        )

    total = table.num_rows
    parsed = sum(1 for v in table["parse_success"].to_pylist() if v)
    datasetable = sum(1 for v in table["dataset_success"].to_pylist() if v)
    succeeded = sum(1 for v in table["success"].to_pylist() if v)

    if this_run == 0 and total >= total_granules:
        return (
            f"attempt: 0 new attempts "
            f"(all {total_granules} sampled granules already have results)"
        )

    pending = total_granules - total
    if this_run == 0 and pending > 0:
        return (
            f"attempt: 0 new attempts "
            f"({pending} granules still pending, {total} already have results). "
            f"If you expected work to happen, check the logs above."
        )

    return (
        f"attempt: {this_run} new attempts "
        f"({total} of {total_granules} total granules complete; "
        f"{parsed} parsed, {datasetable} datasetable, {succeeded} fully succeeded)"
    )
