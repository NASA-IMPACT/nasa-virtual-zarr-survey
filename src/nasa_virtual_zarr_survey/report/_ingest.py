"""DuckDB / Parquet ingestion helpers for the report module.

The report needs two things from the on-disk survey state:

1. A view named ``results`` over every Parquet shard under ``results_dir``,
   optionally filtered to a cached subset.
2. A temp DuckDB table of granule IDs whose URL is on disk in a cache
   directory (used by ``--cache-only``).

Both helpers are stateful (they create views/tables on a connection) and
isolated here so the aggregation layer can stay pure-ish.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def register_cached_granules(con: duckdb.DuckDBPyConnection, cache_dir: Path) -> str:
    """Build a temp DuckDB table of granule IDs whose URL is on disk in *cache_dir*.

    Reads ``(granule_concept_id, data_url)`` from the ``granules`` table, keeps
    rows where ``cache_layout_path(cache_dir, data_url)`` exists, and exposes
    them as ``_cached_granules``. Returns the table name. Logs the keep ratio
    to stderr to mirror the ``attempt --cache-only`` log line.
    """
    from nasa_virtual_zarr_survey.cache import cache_layout_path

    rows = con.execute(
        "SELECT granule_concept_id, data_url FROM granules WHERE data_url IS NOT NULL"
    ).fetchall()
    total = len(rows)
    kept_ids: list[str] = []
    for gid, url in rows:
        try:
            if cache_layout_path(cache_dir, url).exists():
                kept_ids.append(gid)
        except ValueError:
            continue
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _cached_granules (granule_concept_id TEXT)"
    )
    if kept_ids:
        con.executemany(
            "INSERT INTO _cached_granules VALUES (?)",
            [(gid,) for gid in kept_ids],
        )
    print(
        f"report: --cache-only kept {len(kept_ids)} of {total} granule(s) "
        f"found in {cache_dir}",
        file=sys.stderr,
        flush=True,
    )
    return "_cached_granules"


def attach_results(
    con: duckdb.DuckDBPyConnection,
    results_dir: Path,
    cache_filter_table: str | None = None,
) -> bool:
    """Register a view ``results`` over all Parquet shards. Returns True if any exist.

    When ``cache_filter_table`` is set, the view is restricted to granules whose
    ``granule_concept_id`` appears in that table (used by ``--cache-only``).
    """
    shards = list(results_dir.glob("**/*.parquet"))
    if not shards:
        con.execute(
            "CREATE OR REPLACE VIEW results AS SELECT * FROM (VALUES (NULL)) WHERE false"
        )
        return False
    glob = str(results_dir / "**" / "*.parquet")
    base = f"SELECT * FROM read_parquet('{glob}', union_by_name=true, hive_partitioning=true)"
    if cache_filter_table:
        base += (
            f" WHERE granule_concept_id IN "
            f"(SELECT granule_concept_id FROM {cache_filter_table})"
        )
    con.execute(f"CREATE OR REPLACE VIEW results AS {base}")
    return True
