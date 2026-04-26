from pathlib import Path

import duckdb
import pytest

from nasa_virtual_zarr_survey.db import connect, init_schema
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
                "temporal_bin",
                "size_bytes",
                "sampled_at",
                "stratified",
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
            con, "C1", "G1", data_url="s3://y", temporal_bin=1, size_bytes=200
        )


@pytest.mark.parametrize("table", ["collections", "granules"])
def test_table_has_umm_json_column(tmp_db_path: Path, table: str):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {r[1]: r[2] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    assert "umm_json" in cols
    assert cols["umm_json"].upper() == "JSON"


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
            temporal_bin          INTEGER,
            size_bytes            BIGINT,
            sampled_at            TIMESTAMP,
            stratified            BOOLEAN,
            access_mode           TEXT NOT NULL,
            PRIMARY KEY (collection_concept_id, granule_concept_id)
        )
    """)

    with pytest.raises(RuntimeError, match="umm_json"):
        init_schema(con)
