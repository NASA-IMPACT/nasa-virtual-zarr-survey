from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.sample import (
    temporal_bins,
    sample_one_collection,
    run_sample,
)


def test_temporal_bins_splits_evenly():
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bins = temporal_bins(start, end, n=4)
    assert len(bins) == 4
    assert bins[0][0] == start
    assert bins[-1][1] == end
    for (a, b) in bins:
        assert a < b


def test_temporal_bins_none_returns_none():
    assert temporal_bins(None, None, n=5) is None


def test_sample_one_collection_uses_temporal_bins(monkeypatch):
    call_count = {"n": 0}

    class G:
        def __init__(self, gid: str): self.gid = gid
        def __getitem__(self, k):
            return {"meta": {"concept-id": self.gid},
                    "umm": {"DataGranule": {"ArchiveAndDistributionInformation":
                                            [{"SizeInBytes": 100}]}}}[k]
        def data_links(self, access="direct"):
            return [f"s3://b/{self.gid}.nc"]

    def fake_search_data(**kwargs):
        call_count["n"] += 1
        return [G(f"G{call_count['n']}")]

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample.earthaccess.search_data", fake_search_data)

    coll = {
        "concept_id": "C1",
        "time_start": datetime(2020, 1, 1, tzinfo=timezone.utc),
        "time_end": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }
    gs = sample_one_collection(coll, n_bins=5)
    assert len(gs) == 5
    # each bin yielded exactly one search_data call
    assert call_count["n"] == 5
    assert {g["temporal_bin"] for g in gs} == {0, 1, 2, 3, 4}
    assert all(g["stratified"] is True for g in gs)


def test_run_sample_persists_granules(tmp_db_path: Path, monkeypatch):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C1','short','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',10,
         TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00',
         'L3', NULL, now())
    """)

    class G:
        def __init__(self, gid: str): self.gid = gid
        def __getitem__(self, k): return {"meta": {"concept-id": self.gid}}[k]
        def data_links(self, access="direct"): return [f"s3://b/{self.gid}.nc"]

    counter = iter(range(100))
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [G(f"G{next(counter)}")],
    )

    n = run_sample(tmp_db_path, n_bins=3)
    assert n == 3
    rows = con.execute(
        "SELECT collection_concept_id, temporal_bin, stratified FROM granules ORDER BY temporal_bin"
    ).fetchall()
    assert rows == [("C1", 0, True), ("C1", 1, True), ("C1", 2, True)]


def test_sample_one_collection_no_temporal_extent(monkeypatch):
    class G:
        def __init__(self, gid: str): self.gid = gid
        def __getitem__(self, k):
            return {"meta": {"concept-id": self.gid}}[k]
        def data_links(self, access="direct"):
            return [f"s3://b/{self.gid}.nc"]

    captured: dict = {}

    def fake_search_data(**kwargs):
        captured.update(kwargs)
        return [G(f"G{i}") for i in range(3)]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data", fake_search_data
    )

    coll = {"concept_id": "C1", "time_start": None, "time_end": None, "num_granules": 1000}
    gs = sample_one_collection(coll, n_bins=3)
    assert len(gs) == 3
    assert {g["temporal_bin"] for g in gs} == {0, 1, 2}
    # fallback should NOT pass `offset` (earthaccess rejects it)
    assert "offset" not in captured
    # it should request count=n_bins in a single call
    assert captured.get("count") == 3
    assert captured.get("concept_id") == "C1"
    assert all(g["stratified"] is False for g in gs)


def test_sample_one_collection_external_access(monkeypatch):
    captured_accesses = []

    class G:
        def __init__(self, gid): self.gid = gid
        def __getitem__(self, k): return {"meta": {"concept-id": self.gid}}[k]
        def data_links(self, access="direct"):
            captured_accesses.append(access)
            return [f"https://ex/{self.gid}.nc"]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [G("G1")],
    )

    coll = {"concept_id": "C1", "time_start": None, "time_end": None, "num_granules": 1}
    gs = sample_one_collection(coll, n_bins=1, access="external")
    assert gs[0]["data_url"] == "https://ex/G1.nc"
    assert "external" in captured_accesses
