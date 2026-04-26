from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.sample import (
    _granule_format,
    _update_collection_classification,
    run_sample,
    sample_one_collection,
    temporal_bins,
)
from tests.conftest import insert_collection, insert_granule, make_fake_granule


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

    def fake_search_data(**kwargs):
        call_count["n"] += 1
        gid = f"G{call_count['n']}"
        return [
            make_fake_granule(
                gid,
                umm={
                    "DataGranule": {
                        "ArchiveAndDistributionInformation": [{"SizeInBytes": 100}]
                    }
                },
            )
        ]

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
    insert_collection(
        con,
        "C1",
        num_granules=10,
        time_start=datetime(2020, 1, 1),
        time_end=datetime(2024, 1, 1),
    )

    counter = iter(range(100))
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [make_fake_granule(f"G{next(counter)}")],
    )

    n = run_sample(tmp_db_path, n_bins=3)
    assert n == 3
    rows = con.execute(
        "SELECT collection_concept_id, temporal_bin, stratified FROM granules ORDER BY temporal_bin"
    ).fetchall()
    assert rows == [("C1", 0, True), ("C1", 1, True), ("C1", 2, True)]


def test_sample_one_collection_no_temporal_extent(monkeypatch):
    captured: dict = {}

    def fake_search_data(**kwargs):
        captured.update(kwargs)
        return [make_fake_granule(f"G{i}") for i in range(3)]

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
    captured_accesses: list[str] = []

    def url_for(access: str) -> list[str]:
        captured_accesses.append(access)
        return ["https://ex/G1.nc"]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [make_fake_granule("G1", urls=url_for)],
    )

    coll = {"concept_id": "C1", "time_start": None, "time_end": None, "num_granules": 1}
    gs = sample_one_collection(coll, n_bins=1, access="external")
    assert gs[0]["data_url"] == "https://ex/G1.nc"
    # When access="external", the chosen URL is also the HTTPS URL — no
    # second data_links() call should be needed.
    assert gs[0]["https_url"] == "https://ex/G1.nc"
    assert "external" in captured_accesses


# ---------------------------------------------------------------------------
# _granule_format
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "archive_info, expected",
    [
        ([{"Format": "NetCDF-4"}], "NetCDF-4"),
        ({"Format": "HDF5"}, "HDF5"),
        ({}, None),
    ],
    ids=["list", "dict", "missing"],
)
def test_granule_format(archive_info, expected):
    class G:
        def __getitem__(self, k):
            return {
                "umm": (
                    {"DataGranule": {"ArchiveAndDistributionInformation": archive_info}}
                    if archive_info
                    else {}
                )
            }[k]

    assert _granule_format(G()) == expected


# ---------------------------------------------------------------------------
# _update_collection_classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "probed_format, expected_resolved, expected_row",
    [
        # Array-like format → unblocked.
        ("NetCDF-4", None, ("NetCDF4", "NetCDF-4", None)),
        # Non-array format → mark as such, stays skipped.
        ("PDF", "non_array_format", (None, "PDF", "non_array_format")),
        # Probe also yielded nothing → still unknown, still skipped.
        (None, "format_unknown", (None, None, "format_unknown")),
    ],
    ids=["array", "non_array", "still_unknown"],
)
def test_update_collection_classification(
    tmp_db_path: Path,
    probed_format: str | None,
    expected_resolved: str | None,
    expected_row: tuple,
):
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(
        con,
        "C1",
        format_family=None,
        format_declared=None,
        num_granules=5,
        time_start=datetime(2020, 1, 1),
        time_end=datetime(2024, 1, 1),
        skip_reason="format_unknown",
    )
    resolved = _update_collection_classification(con, "C1", probed_format)
    assert resolved == expected_resolved
    row = con.execute(
        "SELECT format_family, format_declared, skip_reason FROM collections WHERE concept_id='C1'"
    ).fetchone()
    assert row == expected_row


# ---------------------------------------------------------------------------
# run_sample re-classification path
# ---------------------------------------------------------------------------


def test_run_sample_reclassifies_format_unknown(tmp_db_path: Path, monkeypatch):
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(
        con,
        "C_UNKNOWN",
        short_name="shortname",
        format_family=None,
        format_declared=None,
        num_granules=5,
        time_start=datetime(2020, 1, 1),
        time_end=datetime(2024, 1, 1),
        skip_reason="format_unknown",
    )

    counter = iter(range(100))
    fmt_umm = {
        "DataGranule": {"ArchiveAndDistributionInformation": [{"Format": "NetCDF-4"}]}
    }
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [make_fake_granule(f"G{next(counter)}", umm=fmt_umm)],
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
    insert_collection(
        con,
        "C_PDF",
        short_name="n",
        format_family=None,
        format_declared=None,
        num_granules=5,
        time_start=datetime(2020, 1, 1),
        time_end=datetime(2024, 1, 1),
        skip_reason="format_unknown",
    )

    pdf_umm = {
        "DataGranule": {"ArchiveAndDistributionInformation": [{"Format": "PDF"}]}
    }
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [
            make_fake_granule("GP", umm=pdf_umm, urls=["s3://b/whatever.pdf"])
        ],
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
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(
        con,
        "C1",
        num_granules=2,
        time_start=datetime(2020, 1, 1),
        time_end=datetime(2024, 1, 1),
    )
    # Pre-populate with direct-mode rows.
    insert_granule(
        con, "C1", "G0", data_url="s3://b/0.nc", size_bytes=100, stratified=False
    )
    insert_granule(
        con,
        "C1",
        "G1",
        data_url="s3://b/1.nc",
        temporal_bin=1,
        size_bytes=100,
        stratified=False,
    )

    counter = iter(range(100))
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [
            make_fake_granule(
                f"NEW{next(counter)}",
                urls=lambda access: ["https://ex/NEW.nc"],
            )
        ],
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


def test_sample_one_collection_captures_umm_json(monkeypatch):
    """Each GranuleInfo should carry the full ``{meta, umm}`` dict."""
    granule_umm = {
        "GranuleUR": "FOO.nc",
        "DataGranule": {
            "ArchiveAndDistributionInformation": [
                {"Format": "NetCDF-4", "SizeInBytes": 1024}
            ]
        },
    }
    fake = make_fake_granule(
        "G1-PODAAC",
        umm=granule_umm,
        urls=["s3://bucket/FOO.nc"],
        with_render_dict=True,
    )

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **kw: [fake],
    )
    coll = {
        "concept_id": "C1-PODAAC",
        "time_start": datetime(2020, 1, 1, tzinfo=timezone.utc),
        "time_end": datetime(2020, 1, 2, tzinfo=timezone.utc),
    }
    rows = sample_one_collection(coll, n_bins=1, access="direct")
    assert len(rows) == 1
    assert rows[0]["umm_json"] == {
        "meta": {"concept-id": "G1-PODAAC"},
        "umm": granule_umm,
    }


def test_run_sample_round_trips_granule_umm_json(tmp_db_path: Path, monkeypatch):
    """``run_sample`` should persist the granule blob so json_extract works."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(
        con,
        "C1",
        short_name="short",
        time_start=datetime(2020, 1, 1),
        time_end=datetime(2024, 1, 1),
    )

    granule_umm = {
        "GranuleUR": "FOO.nc",
        "DataGranule": {"ArchiveAndDistributionInformation": [{"Format": "NetCDF-4"}]},
    }
    fake = make_fake_granule(
        "G1-PODAAC",
        umm=granule_umm,
        urls=["s3://bucket/FOO.nc"],
        with_render_dict=True,
    )

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [fake],
    )

    n = run_sample(tmp_db_path, n_bins=1, access="direct")
    assert n == 1

    granule_ur = con.execute(
        "SELECT json_extract(umm_json, '$.umm.GranuleUR') FROM granules "
        "WHERE collection_concept_id = 'C1'"
    ).fetchone()[0]
    # DuckDB returns json_extract results as quoted JSON strings.
    assert granule_ur == '"FOO.nc"'


def test_run_sample_skips_when_already_in_requested_mode(
    tmp_db_path: Path, monkeypatch
):
    """Existing rows in the requested mode are kept; no re-sample, no warning."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1")
    insert_granule(
        con, "C1", "G0", data_url="s3://b/0.nc", size_bytes=100, stratified=False
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


# ---------------------------------------------------------------------------
# dmrpp_granule_url
# ---------------------------------------------------------------------------


def test_sample_one_collection_records_dmrpp_url_for_opendap_collection(
    monkeypatch,
):
    """When the collection has cloud OPeNDAP, dmrpp_granule_url = https_url + .dmrpp."""
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [
            make_fake_granule(
                "G1",
                urls=lambda access: ["s3://b/G1.h5"]
                if access == "direct"
                else ["https://x/G1.h5"],
            )
        ],
    )
    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 1,
        "has_cloud_opendap": True,
    }
    rows = sample_one_collection(coll, n_bins=1, access="direct")
    assert rows[0]["data_url"] == "s3://b/G1.h5"
    assert rows[0]["https_url"] == "https://x/G1.h5"
    # Pinned to https_url so the URL is curl-able outside us-west-2.
    assert rows[0]["dmrpp_granule_url"] == "https://x/G1.h5.dmrpp"


def test_sample_one_collection_dmrpp_url_none_when_no_opendap(monkeypatch):
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [make_fake_granule("G1", urls=["s3://b/G1.h5"])],
    )
    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 1,
        "has_cloud_opendap": False,
    }
    assert sample_one_collection(coll, n_bins=1)[0]["dmrpp_granule_url"] is None


def test_sample_one_collection_verify_dmrpp_nulls_on_404(monkeypatch):
    """verify_dmrpp=True clears the URL when the HEAD check fails."""
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [
            make_fake_granule(
                "G1",
                urls=lambda access: ["https://x/G1.h5"],
            )
        ],
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.verify_dmrpp_exists",
        lambda url, **kw: False,
    )
    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 1,
        "has_cloud_opendap": True,
    }
    rows = sample_one_collection(coll, n_bins=1, access="external", verify_dmrpp=True)
    assert rows[0]["dmrpp_granule_url"] is None


def test_sample_one_collection_verify_dmrpp_keeps_on_200(monkeypatch):
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [make_fake_granule("G1", urls=lambda access: ["https://x/G1.h5"])],
    )
    seen: list[str] = []

    def _verify(url, **kw):
        seen.append(url)
        return True

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample.verify_dmrpp_exists", _verify)
    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 1,
        "has_cloud_opendap": True,
    }
    rows = sample_one_collection(coll, n_bins=1, access="external", verify_dmrpp=True)
    assert rows[0]["dmrpp_granule_url"] == "https://x/G1.h5.dmrpp"
    assert seen == ["https://x/G1.h5.dmrpp"]


def test_run_sample_persists_dmrpp_url(tmp_db_path: Path, monkeypatch):
    """run_sample reads has_cloud_opendap from collections and writes the sidecar URL."""
    con = connect(tmp_db_path)
    init_schema(con)
    # Insert a collection that has cloud OPeNDAP.
    con.execute(
        "INSERT INTO collections (concept_id, short_name, daac, format_family, "
        "format_declared, num_granules, time_start, time_end, processing_level, "
        "has_cloud_opendap, discovered_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())",
        [
            "C-OD",
            "name",
            "PODAAC",
            "NetCDF4",
            "NetCDF-4",
            1,
            datetime(2020, 1, 1),
            datetime(2024, 1, 1),
            "L3",
            True,
        ],
    )

    counter = iter(range(100))
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [
            make_fake_granule(
                f"G{next(counter)}",
                urls=lambda access: ["https://x/g.h5"],
            )
        ],
    )

    n = run_sample(tmp_db_path, n_bins=1, access="external")
    assert n == 1
    row = con.execute("SELECT https_url, dmrpp_granule_url FROM granules").fetchone()
    assert row == ("https://x/g.h5", "https://x/g.h5.dmrpp")
