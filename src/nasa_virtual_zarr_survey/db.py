"""DuckDB schema and connection helpers."""

from __future__ import annotations

from pathlib import Path

import duckdb

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS collections (
    concept_id       TEXT PRIMARY KEY,
    short_name       TEXT,
    version          TEXT,
    daac             TEXT,
    provider         TEXT,
    format_family    TEXT,
    format_declared  TEXT,
    num_granules     BIGINT,
    time_start       TIMESTAMP,
    time_end         TIMESTAMP,
    processing_level TEXT,
    skip_reason      TEXT,
    discovered_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS granules (
    collection_concept_id TEXT NOT NULL,
    granule_concept_id    TEXT NOT NULL,
    data_url              TEXT,
    temporal_bin          INTEGER,
    size_bytes            BIGINT,
    sampled_at            TIMESTAMP,
    stratified            BOOLEAN,
    access_mode           TEXT NOT NULL,
    PRIMARY KEY (collection_concept_id, granule_concept_id)
);

CREATE TABLE IF NOT EXISTS run_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_collections_daac ON collections(daac);
CREATE INDEX IF NOT EXISTS idx_granules_collection ON granules(collection_concept_id);
"""


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database at `path`."""
    return duckdb.connect(str(path))


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create tables and indexes if they don't exist. Idempotent.

    Schema changes require deleting `output/survey.duckdb` and re-running.
    """
    con.execute(_SCHEMA_SQL)
