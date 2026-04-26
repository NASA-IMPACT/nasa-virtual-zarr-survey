from pathlib import Path

import duckdb

from nasa_virtual_zarr_survey.db import connect, init_schema


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


def test_collections_columns(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {r[1] for r in con.execute("PRAGMA table_info('collections')").fetchall()}
    for expected in [
        "concept_id",
        "short_name",
        "version",
        "daac",
        "format_family",
        "num_granules",
        "time_start",
        "time_end",
        "skip_reason",
    ]:
        assert expected in cols, f"missing column {expected}"


def test_granules_columns(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {r[1] for r in con.execute("PRAGMA table_info('granules')").fetchall()}
    for expected in [
        "collection_concept_id",
        "granule_concept_id",
        "data_url",
        "temporal_bin",
        "size_bytes",
        "sampled_at",
        "stratified",
    ]:
        assert expected in cols, f"missing column {expected}"


def test_granules_primary_key_prevents_duplicates(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO granules VALUES ('C1', 'G1', 's3://x', NULL, 0, 100, now(), TRUE, 'direct', NULL)"
    )
    # Second insert with same PK should fail (or OR-REPLACE via INSERT OR IGNORE)
    try:
        con.execute(
            "INSERT INTO granules VALUES ('C1', 'G1', 's3://y', NULL, 1, 200, now(), TRUE, 'direct', NULL)"
        )
    except duckdb.ConstraintException:
        pass
    else:
        raise AssertionError("expected ConstraintException")


def test_collections_has_umm_json_column(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {
        r[1]: r[2] for r in con.execute("PRAGMA table_info('collections')").fetchall()
    }
    assert "umm_json" in cols
    assert cols["umm_json"].upper() == "JSON"


def test_granules_has_umm_json_column(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    cols = {r[1]: r[2] for r in con.execute("PRAGMA table_info('granules')").fetchall()}
    assert "umm_json" in cols
    assert cols["umm_json"].upper() == "JSON"


def test_init_schema_raises_on_stale_db(tmp_db_path: Path):
    """A DB whose tables predate the latest schema bump is rejected with a clear message."""
    import pytest

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
