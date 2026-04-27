"""DuckDB schema and connection helpers."""

from __future__ import annotations

from pathlib import Path

import duckdb

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS collections (
    concept_id         TEXT PRIMARY KEY,
    short_name         TEXT,
    version            TEXT,
    daac               TEXT,
    provider           TEXT,
    format_family      TEXT,
    format_declared    TEXT,
    num_granules       BIGINT,
    time_start         TIMESTAMP,
    time_end           TIMESTAMP,
    processing_level   TEXT,
    skip_reason        TEXT,
    has_cloud_opendap  BOOLEAN,
    popularity_rank    INTEGER,
    usage_score        INTEGER,
    discovered_at      TIMESTAMP,
    umm_json           JSON
);

CREATE TABLE IF NOT EXISTS granules (
    collection_concept_id TEXT NOT NULL,
    granule_concept_id    TEXT NOT NULL,
    data_url              TEXT,
    https_url             TEXT,
    dmrpp_granule_url     TEXT,
    stratification_bin    INTEGER,
    n_total_at_sample     BIGINT,
    size_bytes            BIGINT,
    sampled_at            TIMESTAMP,
    access_mode           TEXT NOT NULL,
    umm_json              JSON,
    PRIMARY KEY (collection_concept_id, granule_concept_id)
);

CREATE TABLE IF NOT EXISTS prefetch_log (
    collection_concept_id TEXT NOT NULL,
    granule_concept_id    TEXT NOT NULL,
    action                TEXT NOT NULL,
    status                TEXT NOT NULL,
    size_bytes            BIGINT,
    error                 TEXT,
    ts                    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS run_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMP
);
"""

# Indexes are created after the column check below so that a stale database
# (missing a newly-required column) raises an actionable RuntimeError rather
# than a low-level BinderException from an index referencing the missing column.
_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_collections_daac ON collections(daac);
CREATE INDEX IF NOT EXISTS idx_collections_popularity ON collections(popularity_rank);
CREATE INDEX IF NOT EXISTS idx_granules_collection ON granules(collection_concept_id);
CREATE INDEX IF NOT EXISTS idx_prefetch_log_collection
    ON prefetch_log(collection_concept_id);
"""


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database at `path`."""
    return duckdb.connect(str(path))


# Columns that signal a stale schema when missing. CREATE TABLE IF NOT EXISTS
# is a no-op against a pre-existing table, so a DB created before a schema bump
# silently keeps its old columns; checking these explicitly turns a confusing
# DuckDB binder error at INSERT time into an actionable message at startup.
_REQUIRED_COLUMNS: dict[str, set[str]] = {
    "collections": {
        "umm_json",
        "has_cloud_opendap",
        "popularity_rank",
        "usage_score",
    },
    "granules": {
        "umm_json",
        "dmrpp_granule_url",
        "stratification_bin",
        "n_total_at_sample",
    },
    "prefetch_log": {"action", "status", "ts"},
}


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create tables and indexes if they don't exist. Idempotent.

    Schema changes require deleting `output/survey.duckdb` and re-running.
    """
    con.execute(_SCHEMA_SQL)
    for table, required in _REQUIRED_COLUMNS.items():
        present = {
            r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()
        }
        missing = required - present
        if missing:
            raise RuntimeError(
                f"DuckDB table {table!r} is missing column(s) {sorted(missing)}. "
                "This database predates a schema change. Delete "
                "output/survey.duckdb (and output/results/ if present) and re-run."
            )
    con.execute(_INDEX_SQL)
