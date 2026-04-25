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
        "INSERT INTO granules VALUES ('C1', 'G1', 's3://x', 0, 100, now(), TRUE, 'direct')"
    )
    # Second insert with same PK should fail (or OR-REPLACE via INSERT OR IGNORE)
    try:
        con.execute(
            "INSERT INTO granules VALUES ('C1', 'G1', 's3://y', 1, 200, now(), TRUE, 'direct')"
        )
    except duckdb.ConstraintException:
        pass
    else:
        raise AssertionError("expected ConstraintException")
