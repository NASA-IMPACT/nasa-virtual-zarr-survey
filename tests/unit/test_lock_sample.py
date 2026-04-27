"""Tests for the `lock-sample` subcommand and write_locked_sample helper."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from click.testing import CliRunner

from nasa_virtual_zarr_survey.__main__ import cli
from nasa_virtual_zarr_survey.db import connect, init_schema


def _populate_db(db_path: Path) -> None:
    con = connect(db_path)
    init_schema(con)
    now = datetime.now(timezone.utc)
    con.execute(
        """
        INSERT INTO collections
        (concept_id, daac, provider, format_family, processing_level, short_name,
         version, discovered_at, umm_json)
        VALUES
        ('C2-Y', 'Y.DAAC', 'POCLOUD', 'HDF5', 'L3', 'BAR', '2.1', ?, '{}'),
        ('C1-X', 'X.DAAC', 'PODAAC',  'NETCDF4', 'L4', 'FOO', '1.0', ?, '{}')
        """,
        [now, now],
    )
    con.execute(
        """
        INSERT INTO granules
        (collection_concept_id, granule_concept_id, data_url, stratification_bin,
         size_bytes, access_mode, sampled_at, umm_json)
        VALUES
        ('C1-X', 'G1-X', 's3://b/k1', 0, 100, 'direct', ?, '{}'),
        ('C2-Y', 'G2-Y', 'https://h/k2', 1, NULL, 'external', ?, '{}')
        """,
        [now, now],
    )
    con.execute(
        "INSERT INTO run_meta (key, value, updated_at) VALUES (?, ?, ?)",
        ["sampling_mode", "top=2", now],
    )
    con.close()


def test_lock_sample_writes_canonical_json(tmp_path: Path) -> None:
    db = tmp_path / "survey.duckdb"
    _populate_db(db)
    out = tmp_path / "locked.json"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["lock-sample", "--db", str(db), "--out", str(out)],
    )
    assert result.exit_code == 0, result.output

    data = json.loads(out.read_text())
    assert data["schema_version"] == 3
    assert data["sampling_mode"] == "top=2"
    assert [c["concept_id"] for c in data["collections"]] == ["C1-X", "C2-Y"]
    assert [c["provider"] for c in data["collections"]] == ["PODAAC", "POCLOUD"]
    assert [g["granule_concept_id"] for g in data["granules"]] == ["G1-X", "G2-Y"]
    g0, g1 = data["granules"]
    assert g0["s3_url"] == "s3://b/k1"
    assert g0["https_url"] is None
    assert g1["s3_url"] is None
    assert g1["https_url"] == "https://h/k2"


def test_lock_sample_deterministic(tmp_path: Path) -> None:
    db = tmp_path / "survey.duckdb"
    _populate_db(db)
    out_a = tmp_path / "a.json"
    out_b = tmp_path / "b.json"
    runner = CliRunner()
    runner.invoke(cli, ["lock-sample", "--db", str(db), "--out", str(out_a)])
    runner.invoke(cli, ["lock-sample", "--db", str(db), "--out", str(out_b)])
    a = json.loads(out_a.read_text())
    b = json.loads(out_b.read_text())
    a.pop("created_at")
    b.pop("created_at")
    assert json.dumps(a, sort_keys=False) == json.dumps(b, sort_keys=False)


def test_lock_sample_skips_collections_with_skip_reason(tmp_path: Path) -> None:
    db = tmp_path / "survey.duckdb"
    con = connect(db)
    init_schema(con)
    now = datetime.now(timezone.utc)
    con.execute(
        """
        INSERT INTO collections (concept_id, skip_reason, discovered_at, umm_json)
        VALUES ('C9-X', 'non_array_format', ?, '{}')
        """,
        [now],
    )
    con.close()

    out = tmp_path / "locked.json"
    runner = CliRunner()
    result = runner.invoke(cli, ["lock-sample", "--db", str(db), "--out", str(out)])
    assert result.exit_code == 0, result.output
    data = json.loads(out.read_text())
    assert data["collections"] == []
