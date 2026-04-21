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
                {"RangeDateTimes": [{"BeginningDateTime": "2020-01-01T00:00:00Z",
                                     "EndingDateTime": "2024-01-01T00:00:00Z"}]}
            ],
            "ProcessingLevel": {"Id": "L3"},
        },
    }


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
        def __init__(self, d): self.render_dict = d

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
    assert rows == [("C1-PODAAC", "NetCDF4", None), ("C2-PODAAC", None, "non_array_format")]

    fake_search.assert_called_once()
    kwargs = fake_search.call_args.kwargs
    assert kwargs["cloud_hosted"] is True
    assert "provider" in kwargs
