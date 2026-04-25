from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.sample import (
    temporal_bins,
    sample_one_collection,
    run_sample,
    _granule_format,
    _update_collection_classification,
)


def test_temporal_bins_splits_evenly():
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bins = temporal_bins(start, end, n=4)
    assert len(bins) == 4
    assert bins[0][0] == start
    assert bins[-1][1] == end
    for a, b in bins:
        assert a < b


def test_temporal_bins_none_returns_none():
    assert temporal_bins(None, None, n=5) is None


def test_sample_one_collection_uses_temporal_bins(monkeypatch):
    call_count = {"n": 0}

    class G:
        def __init__(self, gid: str):
            self.gid = gid

        def __getitem__(self, k):
            return {
                "meta": {"concept-id": self.gid},
                "umm": {
                    "DataGranule": {
                        "ArchiveAndDistributionInformation": [{"SizeInBytes": 100}]
                    }
                },
            }[k]

        def data_links(self, access="direct"):
            return [f"s3://b/{self.gid}.nc"]

    def fake_search_data(**kwargs):
        call_count["n"] += 1
        return [G(f"G{call_count['n']}")]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data", fake_search_data
    )

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
        def __init__(self, gid: str):
            self.gid = gid

        def __getitem__(self, k):
            return {"meta": {"concept-id": self.gid}}[k]

        def data_links(self, access="direct"):
            return [f"s3://b/{self.gid}.nc"]

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
        def __init__(self, gid: str):
            self.gid = gid

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

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 1000,
    }
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
        def __init__(self, gid):
            self.gid = gid

        def __getitem__(self, k):
            return {"meta": {"concept-id": self.gid}}[k]

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


# ---------------------------------------------------------------------------
# _granule_format
# ---------------------------------------------------------------------------


def test_granule_format_list():
    class G:
        def __getitem__(self, k):
            return {
                "umm": {
                    "DataGranule": {
                        "ArchiveAndDistributionInformation": [{"Format": "NetCDF-4"}]
                    }
                }
            }[k]

    assert _granule_format(G()) == "NetCDF-4"


def test_granule_format_dict():
    class G:
        def __getitem__(self, k):
            return {
                "umm": {
                    "DataGranule": {
                        "ArchiveAndDistributionInformation": {"Format": "HDF5"}
                    }
                }
            }[k]

    assert _granule_format(G()) == "HDF5"


def test_granule_format_missing():
    class G:
        def __getitem__(self, k):
            return {"umm": {}}[k]

    assert _granule_format(G()) is None


# ---------------------------------------------------------------------------
# _update_collection_classification
# ---------------------------------------------------------------------------


def test_update_collection_classification_array(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C1','s','1','PODAAC','PODAAC',NULL,NULL,5,
         TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00',
         'L3', 'format_unknown', now())
    """)
    resolved = _update_collection_classification(con, "C1", "NetCDF-4")
    assert resolved is None
    row = con.execute(
        "SELECT format_family, format_declared, skip_reason FROM collections WHERE concept_id='C1'"
    ).fetchone()
    assert row == ("NetCDF4", "NetCDF-4", None)


def test_update_collection_classification_non_array(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C2','s','1','PODAAC','PODAAC',NULL,NULL,5,
         TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00',
         'L3', 'format_unknown', now())
    """)
    resolved = _update_collection_classification(con, "C2", "PDF")
    assert resolved == "non_array_format"
    row = con.execute(
        "SELECT format_family, format_declared, skip_reason FROM collections WHERE concept_id='C2'"
    ).fetchone()
    assert row == (None, "PDF", "non_array_format")


def test_update_collection_classification_still_unknown(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C3','s','1','PODAAC','PODAAC',NULL,NULL,5,
         TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00',
         'L3', 'format_unknown', now())
    """)
    resolved = _update_collection_classification(con, "C3", None)
    assert resolved == "format_unknown"
    row = con.execute(
        "SELECT format_family, format_declared, skip_reason FROM collections WHERE concept_id='C3'"
    ).fetchone()
    assert row == (None, None, "format_unknown")


# ---------------------------------------------------------------------------
# run_sample re-classification path
# ---------------------------------------------------------------------------


def test_run_sample_reclassifies_format_unknown(tmp_db_path: Path, monkeypatch):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C_UNKNOWN','shortname','1','PODAAC','PODAAC',NULL,NULL,5,
         TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00',
         'L3', 'format_unknown', now())
    """)

    class G:
        def __init__(self, gid: str, granule_format: str | None = None):
            self.gid = gid
            self._fmt = granule_format

        def __getitem__(self, k):
            if k == "meta":
                return {"concept-id": self.gid}
            if k == "umm":
                if self._fmt:
                    return {
                        "DataGranule": {
                            "ArchiveAndDistributionInformation": [{"Format": self._fmt}]
                        }
                    }
                return {}
            raise KeyError(k)

        def data_links(self, access="direct"):
            return [f"s3://b/{self.gid}.nc"]

    counter = iter(range(100))

    def fake_search_data(**kwargs):
        return [G(f"G{next(counter)}", granule_format="NetCDF-4")]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data", fake_search_data
    )

    n = run_sample(tmp_db_path, n_bins=3)
    assert n == 3

    row = con.execute(
        "SELECT format_family, format_declared, skip_reason FROM collections WHERE concept_id='C_UNKNOWN'"
    ).fetchone()
    assert row == ("NetCDF4", "NetCDF-4", None)


def test_run_sample_skips_unresolvable_format_unknown(tmp_db_path: Path, monkeypatch):
    """If probe yields a non-array format, collection is marked non_array_format and not sampled."""
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C_PDF','n','1','PODAAC','PODAAC',NULL,NULL,5,
         TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00',
         'L3', 'format_unknown', now())
    """)

    class G:
        def __getitem__(self, k):
            if k == "meta":
                return {"concept-id": "GP"}
            if k == "umm":
                return {
                    "DataGranule": {
                        "ArchiveAndDistributionInformation": [{"Format": "PDF"}]
                    }
                }
            raise KeyError(k)

        def data_links(self, access="direct"):
            return ["s3://b/whatever.pdf"]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [G()],
    )

    n = run_sample(tmp_db_path, n_bins=3)
    assert n == 0
    row = con.execute(
        "SELECT format_family, format_declared, skip_reason FROM collections WHERE concept_id='C_PDF'"
    ).fetchone()
    assert row == (None, "PDF", "non_array_format")


# ---------------------------------------------------------------------------
# Re-sample on access mode mismatch
# ---------------------------------------------------------------------------


def test_run_sample_resamples_when_access_mode_changes(
    tmp_db_path: Path, monkeypatch, caplog
):
    """Existing direct-mode rows are deleted and re-sampled when access=external."""
    import logging

    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',2,
         TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00',
         'L3', NULL, now())
    """)
    # Pre-populate with direct-mode rows.
    con.execute(
        "INSERT INTO granules VALUES "
        "('C1','G0','s3://b/0.nc',0,100,now(),FALSE,'direct')"
    )
    con.execute(
        "INSERT INTO granules VALUES "
        "('C1','G1','s3://b/1.nc',1,100,now(),FALSE,'direct')"
    )

    class G:
        def __init__(self, gid: str):
            self.gid = gid

        def __getitem__(self, k):
            return {"meta": {"concept-id": self.gid}}[k]

        def data_links(self, access="direct"):
            return [f"https://ex/{self.gid}.nc"]

    counter = iter(range(100))
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [G(f"NEW{next(counter)}")],
    )

    with caplog.at_level(logging.WARNING, logger="nasa_virtual_zarr_survey.sample"):
        n = run_sample(tmp_db_path, n_bins=2, access="external")

    assert n == 2
    # Old direct-mode rows were deleted; new external-mode rows are present.
    rows = con.execute(
        "SELECT data_url, access_mode FROM granules ORDER BY data_url"
    ).fetchall()
    assert all(url.startswith("https://") for url, _ in rows)
    assert all(mode == "external" for _, mode in rows)

    # Operator was warned about stale parquet rows.
    assert any("re-sampling 1 collection" in r.getMessage() for r in caplog.records)


def test_run_sample_skips_when_already_in_requested_mode(
    tmp_db_path: Path, monkeypatch
):
    """Existing rows in the requested mode are kept; no re-sample, no warning."""

    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',1,
         NULL, NULL, 'L3', NULL, now())
    """)
    con.execute(
        "INSERT INTO granules VALUES "
        "('C1','G0','s3://b/0.nc',0,100,now(),FALSE,'direct')"
    )

    called = {"n": 0}

    def fake_search_data(**_):
        called["n"] += 1
        raise AssertionError("should not search when granules already exist in mode")

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data", fake_search_data
    )

    n = run_sample(tmp_db_path, n_bins=2, access="direct")
    assert n == 0
    assert called["n"] == 0
