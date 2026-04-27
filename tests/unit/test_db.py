from pathlib import Path

import duckdb
import pytest

from nasa_virtual_zarr_survey.db import connect, init_schema, session
from tests.conftest import insert_granule


def test_init_schema_creates_tables(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert {"collections", "granules"}.issubset(tables)


def test_init_schema_is_idempotent(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    init_schema(con)
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert {"collections", "granules"}.issubset(tables)


@pytest.mark.parametrize(
    "table, expected",
    [
        (
            "collections",
            [
                "concept_id",
                "short_name",
                "version",
                "daac",
                "format_family",
                "num_granules",
                "time_start",
                "time_end",
                "skip_reason",
            ],
        ),
        (
            "granules",
            [
                "collection_concept_id",
                "granule_concept_id",
                "data_url",
                "stratification_bin",
                "n_total_at_sample",
                "size_bytes",
                "sampled_at",
            ],
        ),
    ],
)
def test_table_has_expected_columns(tmp_db_path: Path, table: str, expected: list[str]):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    for col in expected:
        assert col in cols, f"missing column {col} in {table}"


def test_granules_primary_key_prevents_duplicates(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    insert_granule(con, "C1", "G1", data_url="s3://x", size_bytes=100)
    # Second insert with the same PK should fail.
    with pytest.raises(duckdb.ConstraintException):
        insert_granule(
            con, "C1", "G1", data_url="s3://y", stratification_bin=1, size_bytes=200
        )


@pytest.mark.parametrize("table", ["collections", "granules"])
def test_table_has_umm_json_column(tmp_db_path: Path, table: str):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {r[1]: r[2] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    assert "umm_json" in cols
    assert cols["umm_json"].upper() == "JSON"


@pytest.mark.parametrize(
    "table, column",
    [
        ("collections", "has_cloud_opendap"),
        ("granules", "dmrpp_granule_url"),
    ],
)
def test_table_has_opendap_columns(tmp_db_path: Path, table: str, column: str):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    assert column in cols


def test_init_schema_raises_on_stale_db(tmp_db_path: Path):
    """A DB whose tables predate the latest schema bump is rejected with a clear message."""
    con = connect(tmp_db_path)
    # Simulate a pre-existing DB created under the schema that predates `umm_json`.
    con.execute("""
        CREATE TABLE collections (
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
        )
    """)
    con.execute("""
        CREATE TABLE granules (
            collection_concept_id TEXT NOT NULL,
            granule_concept_id    TEXT NOT NULL,
            data_url              TEXT,
            https_url             TEXT,
            stratification_bin    INTEGER,
            size_bytes            BIGINT,
            sampled_at            TIMESTAMP,
            access_mode           TEXT NOT NULL,
            PRIMARY KEY (collection_concept_id, granule_concept_id)
        )
    """)

    with pytest.raises(RuntimeError, match="umm_json"):
        init_schema(con)


def test_session_closes_connection_on_exit(tmp_db_path: Path):
    """``session()`` is a context manager that closes its connection."""
    with session(tmp_db_path) as con:
        init_schema(con)
        con.execute("SELECT 1").fetchone()
    # After the block, the connection is closed; further use must fail.
    with pytest.raises(duckdb.ConnectionException):
        con.execute("SELECT 1")


def test_summary_helpers_run_back_to_back(tmp_db_path: Path, tmp_results_dir: Path):
    """The CLI summary helpers must not leak DuckDB connections.

    Regression: previously each helper called ``connect()`` without ever
    closing it. Calling them in sequence in a single process (e.g., a
    long-lived test runner) eventually collided on the .duckdb write lock.
    """
    from nasa_virtual_zarr_survey.cli._summaries import (
        _attempt_summary,
        _discover_summary,
        _sample_summary,
    )

    # Sequential calls in one process must all succeed.
    assert "discover:" in _discover_summary(tmp_db_path)
    assert "sample:" in _sample_summary(tmp_db_path)
    assert "attempt:" in _attempt_summary(tmp_db_path, tmp_results_dir, this_run=0)

    # And after the helpers run, a fresh writable connection must still open.
    con = connect(tmp_db_path)
    try:
        con.execute("CREATE TABLE _probe (x INT)")
    finally:
        con.close()
