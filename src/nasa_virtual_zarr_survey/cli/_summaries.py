"""DB-backed one-line summaries printed at the end of CLI subcommands.

Each function opens its own short-lived DuckDB session so the caller (a
click subcommand) doesn't need to manage one for the summary line.
"""

from __future__ import annotations

from pathlib import Path


def _discover_summary(db_path: Path) -> str:
    from nasa_virtual_zarr_survey.db import init_schema, session

    with session(db_path) as con:
        init_schema(con)
        total = (con.execute("SELECT count(*) FROM collections").fetchone() or (0,))[0]
        skipped = (
            con.execute(
                "SELECT count(*) FROM collections WHERE skip_reason IS NOT NULL"
            ).fetchone()
            or (0,)
        )[0]
    array_like = total - skipped
    return (
        f"discover: {total} collections "
        f"({array_like} array-like, {skipped} skipped as non-array format)"
    )


def _sample_summary(db_path: Path) -> str:
    from nasa_virtual_zarr_survey.db import init_schema, session

    with session(db_path) as con:
        init_schema(con)
        n_gran = (con.execute("SELECT count(*) FROM granules").fetchone() or (0,))[0]
        n_coll = (
            con.execute(
                "SELECT count(DISTINCT collection_concept_id) FROM granules"
            ).fetchone()
            or (0,)
        )[0]
    return f"sample: {n_gran} granules across {n_coll} collections"


def _attempt_summary(db_path: Path, results_dir: Path, this_run: int) -> str:
    from nasa_virtual_zarr_survey.db import init_schema, session

    with session(db_path) as con:
        init_schema(con)
        total_granules = (
            con.execute("SELECT count(*) FROM granules").fetchone() or (0,)
        )[0]

        if total_granules == 0:
            return (
                "attempt: 0 new attempts (the granules table is empty; "
                "run 'sample' or 'discover' first)"
            )

        shards = list(results_dir.glob("**/*.parquet"))
        if not shards:
            if this_run == 0:
                return (
                    f"attempt: 0 new attempts "
                    f"({total_granules} granules pending, 0 results written; "
                    "if you expected attempts to happen, check --daac filters or logs above)"
                )
            return (
                f"attempt: {this_run} new attempts "
                f"(0 of {total_granules} total granules complete; no results written yet)"
            )

        glob = str(results_dir / "**" / "*.parquet")
        q = f"""
            SELECT
                count(*) AS total,
                sum(CASE WHEN parse_success THEN 1 ELSE 0 END) AS parsed,
                sum(CASE WHEN dataset_success THEN 1 ELSE 0 END) AS datasetable,
                sum(CASE WHEN success THEN 1 ELSE 0 END) AS succeeded
            FROM read_parquet('{glob}', union_by_name=true, hive_partitioning=true)
        """
        total, parsed, datasetable, succeeded = con.execute(q).fetchone() or (
            0,
            0,
            0,
            0,
        )
    parsed = parsed or 0
    datasetable = datasetable or 0
    succeeded = succeeded or 0

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
            f"If you expected work to happen, check --daac filter or error above."
        )

    return (
        f"attempt: {this_run} new attempts "
        f"({total} of {total_granules} total granules complete; "
        f"{parsed} parsed, {datasetable} datasetable, {succeeded} fully succeeded)"
    )
