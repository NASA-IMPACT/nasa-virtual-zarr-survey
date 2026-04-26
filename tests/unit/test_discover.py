from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.discover import (
    collection_row_from_umm,
    persist_collections,
    run_discover,
)


def _fake_umm(concept_id: str, fmt: str, provider: str = "PODAAC") -> dict:
    return {
        "meta": {"concept-id": concept_id, "provider-id": provider},
        "umm": {
            "ShortName": "FOO",
            "Version": "1",
            "DataCenters": [{"ShortName": provider}],
            "ArchiveAndDistributionInformation": {
                "FileDistributionInformation": [{"Format": fmt}]
            },
            "TemporalExtents": [
                {
                    "RangeDateTimes": [
                        {
                            "BeginningDateTime": "2020-01-01T00:00:00Z",
                            "EndingDateTime": "2024-01-01T00:00:00Z",
                        }
                    ]
                }
            ],
            "ProcessingLevel": {"Id": "L3"},
        },
    }


def test_collection_row_from_umm_includes_full_blob():
    umm = _fake_umm("C-BLOB-PODAAC", "NetCDF-4")
    row = collection_row_from_umm(umm)
    assert row["umm_json"] == umm
    # The blob is the full top-level dict, including meta, so version info
    # travels with it and no separate version column is needed.
    assert row["umm_json"]["meta"]["concept-id"] == "C-BLOB-PODAAC"
    assert row["umm_json"]["umm"]["ShortName"] == "FOO"


def test_persist_collections_round_trips_umm_json(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    persist_collections(con, [_fake_umm("C-RT-PODAAC", "NetCDF-4")])
    short_name = con.execute(
        "SELECT json_extract(umm_json, '$.umm.ShortName') FROM collections "
        "WHERE concept_id = 'C-RT-PODAAC'"
    ).fetchone()[0]
    # DuckDB returns json_extract results as quoted JSON strings.
    assert short_name == '"FOO"'


def test_collection_row_from_umm_parses_netcdf():
    umm = _fake_umm("C123-PODAAC", "NetCDF-4")
    row = collection_row_from_umm(umm)
    assert row["concept_id"] == "C123-PODAAC"
    assert row["short_name"] == "FOO"
    assert row["version"] == "1"
    assert row["daac"] == "PODAAC"
    assert row["format_family"] == "NetCDF4"
    assert row["skip_reason"] is None
    assert isinstance(row["time_start"], datetime)


def test_collection_row_from_umm_marks_pdf_skipped():
    umm = _fake_umm("C456-PODAAC", "PDF")
    row = collection_row_from_umm(umm)
    assert row["format_family"] is None
    assert row["skip_reason"] == "non_array_format"


def test_persist_collections_upserts(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    umm_rows = [_fake_umm("C1-PODAAC", "NetCDF-4"), _fake_umm("C2-PODAAC", "PDF")]
    persist_collections(con, umm_rows)
    n = con.execute("SELECT count(*) FROM collections").fetchone()[0]
    assert n == 2
    # Re-run with the same data: still 2
    persist_collections(con, umm_rows)
    n = con.execute("SELECT count(*) FROM collections").fetchone()[0]
    assert n == 2


def test_run_discover_uses_earthaccess(tmp_db_path: Path, monkeypatch):
    fake_results = [_fake_umm("C1-PODAAC", "NetCDF-4"), _fake_umm("C2-PODAAC", "PDF")]

    # earthaccess returns DataCollection objects; render_dict is an attribute, not a method
    class FakeColl:
        def __init__(self, d):
            self.render_dict = d

    fake_search = MagicMock(return_value=[FakeColl(d) for d in fake_results])
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.discover.earthaccess.search_datasets", fake_search
    )
    run_discover(tmp_db_path, limit=100)

    con = connect(tmp_db_path)
    init_schema(con)
    rows = con.execute(
        "SELECT concept_id, format_family, skip_reason FROM collections ORDER BY concept_id"
    ).fetchall()
    assert rows == [
        ("C1-PODAAC", "NetCDF4", None),
        ("C2-PODAAC", None, "non_array_format"),
    ]

    fake_search.assert_called_once()
    kwargs = fake_search.call_args.kwargs
    assert kwargs["cloud_hosted"] is True
    assert "provider" in kwargs


def test_collection_row_from_umm_uses_file_archive_information():
    umm = {
        "meta": {"concept-id": "C1", "provider-id": "PODAAC"},
        "umm": {
            "ShortName": "FOO",
            "Version": "1",
            "DataCenters": [{"ShortName": "PODAAC"}],
            "ArchiveAndDistributionInformation": {
                # No FileDistributionInformation; format only in FileArchiveInformation.
                "FileArchiveInformation": [{"Format": "NetCDF-4"}],
            },
            "ProcessingLevel": {"Id": "L3"},
        },
    }
    row = collection_row_from_umm(umm)
    assert row["format_family"] == "NetCDF4"
    assert row["skip_reason"] is None


def test_collection_row_from_umm_marks_null_format_as_unknown():
    umm = {
        "meta": {"concept-id": "C2", "provider-id": "PODAAC"},
        "umm": {
            "ShortName": "BAR",
            "Version": "1",
            "DataCenters": [{"ShortName": "PODAAC"}],
            "ArchiveAndDistributionInformation": {},
            "ProcessingLevel": {"Id": "L3"},
        },
    }
    row = collection_row_from_umm(umm)
    assert row["format_family"] is None
    assert row["format_declared"] is None
    assert row["skip_reason"] == "format_unknown"


def test_collection_row_from_umm_marks_l1_as_processing_level_skip():
    umm = _fake_umm("C-L1", "NetCDF-4")
    umm["umm"]["ProcessingLevel"] = {"Id": "L1B"}
    row = collection_row_from_umm(umm)
    assert row["processing_level"] == "L1B"
    assert row["skip_reason"] == "processing_level"
    # format_family is still classified so the reason for the skip is unambiguous
    assert row["format_family"] == "NetCDF4"


def test_collection_row_from_umm_processing_level_takes_precedence_over_format_unknown():
    umm = {
        "meta": {"concept-id": "C-L0", "provider-id": "PODAAC"},
        "umm": {
            "ShortName": "L0",
            "Version": "1",
            "DataCenters": [{"ShortName": "PODAAC"}],
            "ArchiveAndDistributionInformation": {},
            "ProcessingLevel": {"Id": "0"},
        },
    }
    row = collection_row_from_umm(umm)
    assert row["skip_reason"] == "processing_level"


def test_collection_row_from_umm_non_array_format_stays_non_array():
    umm = {
        "meta": {"concept-id": "C3", "provider-id": "PODAAC"},
        "umm": {
            "ShortName": "BAZ",
            "Version": "1",
            "DataCenters": [{"ShortName": "PODAAC"}],
            "ArchiveAndDistributionInformation": {
                "FileDistributionInformation": [{"Format": "PDF"}],
            },
        },
    }
    row = collection_row_from_umm(umm)
    assert row["format_family"] is None
    assert row["format_declared"] == "PDF"
    assert row["skip_reason"] == "non_array_format"


def test_run_discover_top_per_provider_hydrates_ids(tmp_db_path: Path, monkeypatch):
    from nasa_virtual_zarr_survey.discover import run_discover

    # Mock the popularity module's entry point used by discover.
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.popularity.all_top_collection_ids",
        lambda providers, num_per_provider=100: ["C1-PODAAC", "C2-PODAAC"],
    )

    class FakeColl:
        def __init__(self, concept_id):
            self.concept_id = concept_id
            self.render_dict = {
                "meta": {"concept-id": concept_id, "provider-id": "PODAAC"},
                "umm": {
                    "ShortName": "FOO",
                    "Version": "1",
                    "DataCenters": [{"ShortName": "PODAAC"}],
                    "ArchiveAndDistributionInformation": {
                        "FileDistributionInformation": [{"Format": "NetCDF-4"}]
                    },
                    "ProcessingLevel": {"Id": "L3"},
                },
            }

    captured: list = []

    def fake_search(**kwargs):
        captured.append(kwargs)
        return [FakeColl(cid) for cid in kwargs["concept_id"]]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.discover.earthaccess.search_datasets", fake_search
    )
    run_discover(tmp_db_path, top_per_provider=2)

    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(tmp_db_path)
    init_schema(con)
    ids = sorted(
        r[0] for r in con.execute("SELECT concept_id FROM collections").fetchall()
    )
    assert ids == ["C1-PODAAC", "C2-PODAAC"]
    # Verify earthaccess was called with concept_id= (not cloud_hosted= mode)
    assert captured[0]["concept_id"] == ["C1-PODAAC", "C2-PODAAC"]
    assert "cloud_hosted" not in captured[0]
