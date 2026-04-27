"""Hint emitted when ``repro <CONCEPT_ID>`` finds nothing to reproduce.

Tells the operator to use ``probe`` instead, when the collection was either
filtered out at discover time or never sampled into the granules table.
"""

from __future__ import annotations

from pathlib import Path


def _probe_hint(db_path: Path, results_dir: Path, concept_id: str) -> str | None:
    """Return a 'try probe' hint when the concept ID is skipped or un-attempted.

    Triggers when:
    - the collection has ``skip_reason IS NOT NULL`` (no granules attempted), OR
    - the concept ID has zero rows in granules AND zero rows in the Parquet log.

    Otherwise returns None.
    """
    if not db_path.exists():
        return None

    from nasa_virtual_zarr_survey.db import connect, init_schema

    is_collection = concept_id.startswith("C")
    is_granule = concept_id.startswith("G")
    if not (is_collection or is_granule):
        return None

    con = connect(db_path)
    try:
        init_schema(con)
        skip_reason: str | None = None
        if is_collection:
            row = con.execute(
                "SELECT skip_reason FROM collections WHERE concept_id = ?",
                [concept_id],
            ).fetchone()
            if row is not None and row[0] is not None:
                skip_reason = row[0]

        if is_collection:
            n_gran = (
                con.execute(
                    "SELECT count(*) FROM granules WHERE collection_concept_id = ?",
                    [concept_id],
                ).fetchone()
                or (0,)
            )[0]
        else:
            n_gran = (
                con.execute(
                    "SELECT count(*) FROM granules WHERE granule_concept_id = ?",
                    [concept_id],
                ).fetchone()
                or (0,)
            )[0]
    finally:
        con.close()

    n_parquet = 0
    shards = list(results_dir.glob("**/*.parquet"))
    if shards:
        import duckdb

        con2 = duckdb.connect(":memory:")
        col = "collection_concept_id" if is_collection else "granule_concept_id"
        glob = str(results_dir / "**" / "*.parquet")
        try:
            n_parquet = (
                con2.execute(
                    f"SELECT count(*) FROM read_parquet({glob!r}, "
                    f"union_by_name=true, hive_partitioning=true) WHERE {col} = ?",
                    [concept_id],
                ).fetchone()
                or (0,)
            )[0]
        except Exception:
            n_parquet = 0
        finally:
            con2.close()

    if skip_reason is not None:
        return (
            f"Hint: this collection has skip_reason={skip_reason!r} "
            "(no granules attempted).\n"
            f"Try `nasa-virtual-zarr-survey probe {concept_id}` to investigate."
        )
    if n_gran == 0 and n_parquet == 0:
        kind = "collection" if is_collection else "granule"
        return (
            f"Hint: this {kind} has no granules sampled and no Parquet rows.\n"
            f"Try `nasa-virtual-zarr-survey probe {concept_id}` to investigate."
        )
    return None
