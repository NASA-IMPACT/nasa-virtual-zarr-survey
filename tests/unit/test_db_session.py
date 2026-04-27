"""Tests for nasa_virtual_zarr_survey.db_session.SurveySession."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nasa_virtual_zarr_survey.db_session import SurveySession


def test_from_duckdb_creates_schema(tmp_path: Path) -> None:
    db = tmp_path / "survey.duckdb"
    session = SurveySession.from_duckdb(db)
    assert session.con.execute("SELECT count(*) FROM collections").fetchone() == (0,)
    assert session.con.execute("SELECT count(*) FROM granules").fetchone() == (0,)


def test_from_duckdb_persists(tmp_path: Path) -> None:
    db = tmp_path / "survey.duckdb"
    s1 = SurveySession.from_duckdb(db)
    s1.con.execute("INSERT INTO collections (concept_id) VALUES (?)", ["C1-TEST"])
    s1.con.close()
    s2 = SurveySession.from_duckdb(db)
    assert s2.con.execute("SELECT concept_id FROM collections").fetchone() == (
        "C1-TEST",
    )


def _sample_payload() -> dict:
    return {
        "schema_version": 3,
        "created_at": "2026-04-26T12:00:00Z",
        "sampling_mode": "top=2",
        "collections": [
            {
                "concept_id": "C1-T",
                "daac": "X.DAAC",
                "provider": "PODAAC",
                "format_family": "NETCDF4",
                "processing_level": "L4",
                "short_name": "FOO",
                "version": "1.0",
            },
        ],
        "granules": [
            {
                "collection_concept_id": "C1-T",
                "granule_concept_id": "G1-T",
                "s3_url": "s3://b/k1",
                "https_url": "https://h/k1",
                "stratification_bin": 0,
                "n_total_at_sample": 100,
                "size_bytes": 100,
            },
        ],
    }


def test_from_locked_sample_direct(tmp_path: Path) -> None:
    path = tmp_path / "locked.json"
    path.write_text(json.dumps(_sample_payload()))
    session = SurveySession.from_locked_sample(path, access="direct")
    row = session.con.execute("SELECT data_url, access_mode FROM granules").fetchone()
    assert row == ("s3://b/k1", "direct")


def test_from_locked_sample_external(tmp_path: Path) -> None:
    path = tmp_path / "locked.json"
    path.write_text(json.dumps(_sample_payload()))
    session = SurveySession.from_locked_sample(path, access="external")
    row = session.con.execute("SELECT data_url, access_mode FROM granules").fetchone()
    assert row == ("https://h/k1", "external")


def test_from_locked_sample_skips_when_url_missing(tmp_path: Path) -> None:
    payload = _sample_payload()
    payload["granules"][0]["s3_url"] = None
    path = tmp_path / "locked.json"
    path.write_text(json.dumps(payload))
    session = SurveySession.from_locked_sample(path, access="direct")
    assert session.con.execute("SELECT count(*) FROM granules").fetchone() == (0,)


def test_from_locked_sample_rejects_unknown_schema(tmp_path: Path) -> None:
    path = tmp_path / "locked.json"
    path.write_text(
        json.dumps({"schema_version": 99, "collections": [], "granules": []})
    )
    with pytest.raises(ValueError, match="Unsupported locked_sample schema_version"):
        SurveySession.from_locked_sample(path, access="direct")


def test_from_locked_sample_collections_loaded(tmp_path: Path) -> None:
    path = tmp_path / "locked.json"
    path.write_text(json.dumps(_sample_payload()))
    session = SurveySession.from_locked_sample(path, access="direct")
    rows = session.con.execute(
        "SELECT concept_id, daac, provider, format_family, processing_level "
        "FROM collections"
    ).fetchall()
    assert rows == [("C1-T", "X.DAAC", "PODAAC", "NETCDF4", "L4")]
