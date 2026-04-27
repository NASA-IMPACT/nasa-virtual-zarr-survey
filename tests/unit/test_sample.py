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
)
from tests.conftest import insert_collection, insert_granule, make_fake_granule


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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(f"G{next(counter)}"),
    )

    n = run_sample(tmp_db_path, n_bins=3)
    assert n == 3
    rows = con.execute(
        "SELECT collection_concept_id, stratification_bin FROM granules ORDER BY stratification_bin"
    ).fetchall()
    assert rows == [("C1", 0), ("C1", 1), ("C1", 2)]


def test_sample_one_collection_external_access(monkeypatch):
    captured_accesses: list[str] = []

    def url_for(access: str) -> list[str]:
        captured_accesses.append(access)
        return ["https://ex/G1.nc"]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule("G1", urls=url_for),
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
    # Probe uses earthaccess.search_data; sampling uses _fetch_with_retry.
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [make_fake_granule(f"G{next(counter)}", umm=fmt_umm)],
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            f"G{next(counter)}", umm=fmt_umm
        ),
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
    # Probe uses earthaccess.search_data; sampling uses _fetch_with_retry.
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.earthaccess.search_data",
        lambda **_: [
            make_fake_granule("GP", umm=pdf_umm, urls=["s3://b/whatever.pdf"])
        ],
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            "GP", umm=pdf_umm, urls=["s3://b/whatever.pdf"]
        ),
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
    insert_granule(con, "C1", "G0", data_url="s3://b/0.nc", size_bytes=100)
    insert_granule(
        con,
        "C1",
        "G1",
        data_url="s3://b/1.nc",
        stratification_bin=1,
        size_bytes=100,
    )

    counter = iter(range(100))
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            f"NEW{next(counter)}",
            urls=lambda access: ["https://ex/NEW.nc"],
        ),
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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: fake,
    )
    coll = {
        "concept_id": "C1-PODAAC",
        "time_start": datetime(2020, 1, 1, tzinfo=timezone.utc),
        "time_end": datetime(2020, 1, 2, tzinfo=timezone.utc),
        "num_granules": 1,
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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: fake,
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
    insert_granule(con, "C1", "G0", data_url="s3://b/0.nc", size_bytes=100)

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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            "G1",
            urls=lambda access: ["s3://b/G1.h5"]
            if access == "direct"
            else ["https://x/G1.h5"],
        ),
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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            "G1", urls=["s3://b/G1.h5"]
        ),
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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            "G1",
            urls=lambda access: ["https://x/G1.h5"],
        ),
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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            "G1", urls=lambda access: ["https://x/G1.h5"]
        ),
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
        "nasa_virtual_zarr_survey.sample._fetch_with_retry",
        lambda concept_id, offset, *, bin_index: make_fake_granule(
            f"G{next(counter)}",
            urls=lambda access: ["https://x/g.h5"],
        ),
    )

    n = run_sample(tmp_db_path, n_bins=1, access="external")
    assert n == 1
    row = con.execute("SELECT https_url, dmrpp_granule_url FROM granules").fetchone()
    assert row == ("https://x/g.h5", "https://x/g.h5.dmrpp")


def test_hits_reads_cmr_hits_header(monkeypatch):
    from nasa_virtual_zarr_survey.sample import _hits

    captured: dict = {}

    class FakeResponse:
        status_code = 200
        headers = {"cmr-hits": "12345"}

        def raise_for_status(self):
            pass

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        return FakeResponse()

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample.requests.get", fake_get)

    n = _hits("C1234-PROV")
    assert n == 12345
    assert captured["url"].endswith("/search/granules.umm_json")
    assert captured["params"]["collection_concept_id"] == "C1234-PROV"
    assert captured["params"]["page_size"] == 0


def test_fetch_at_offset_returns_data_granule(monkeypatch):
    from earthaccess.results import DataGranule

    from nasa_virtual_zarr_survey.sample import _fetch_at_offset

    captured: dict = {}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {
                "items": [
                    {
                        "meta": {"concept-id": "G1"},
                        "umm": {"RelatedUrls": []},
                    }
                ]
            }

    def fake_get(url, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        return FakeResponse()

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample.requests.get", fake_get)

    g = _fetch_at_offset("C1", offset=200)
    assert isinstance(g, DataGranule)
    assert g["meta"]["concept-id"] == "G1"
    assert captured["params"]["collection_concept_id"] == "C1"
    assert captured["params"]["sort_key"] == "revision_date"
    assert captured["params"]["page_size"] == 1
    assert captured["params"]["page_num"] == 201  # 1-indexed


def test_fetch_at_offset_returns_none_for_empty_response(monkeypatch):
    from nasa_virtual_zarr_survey.sample import _fetch_at_offset

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"items": []}

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.requests.get",
        lambda url, params=None, timeout=None: FakeResponse(),
    )
    assert _fetch_at_offset("C1", offset=999_999) is None


# ---------------------------------------------------------------------------
# New positional-stratification tests (Tasks 8-9)
# ---------------------------------------------------------------------------


def _fake_granule(concept_id: str):
    """Minimal DataGranule-like dict accepted by sample's helpers."""
    from earthaccess.results import DataGranule

    return DataGranule(
        {"meta": {"concept-id": concept_id}, "umm": {"RelatedUrls": []}},
        cloud_hosted=False,
    )


@pytest.mark.parametrize(
    "num_granules, n_bins, expected_offsets",
    [
        (1000, 5, [0, 200, 400, 600, 800]),
        (3, 5, [0, 1, 2]),
        (10, 1, [0]),
    ],
)
def test_stratifies(monkeypatch, num_granules, n_bins, expected_offsets):
    captured_offsets: list[int] = []

    def fake_fetch(concept_id, offset):
        captured_offsets.append(offset)
        return _fake_granule(f"G{offset}")

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample._fetch_at_offset", fake_fetch)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": num_granules,
    }
    rows = sample_one_collection(coll, n_bins=n_bins)
    assert captured_offsets == expected_offsets
    assert len(rows) == len(expected_offsets)
    assert all(r["n_total_at_sample"] == num_granules for r in rows)
    assert [r["stratification_bin"] for r in rows] == list(range(len(expected_offsets)))


def test_stratifies_uses_revision_date_sort(monkeypatch):
    """Verify the wire format: sort_key=revision_date and page_size=1."""
    captured: dict = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "items": [{"meta": {"concept-id": "G0"}, "umm": {"RelatedUrls": []}}]
            }

    def fake_get(url, params=None, timeout=None):
        captured["params"] = params
        return FakeResponse()

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample.requests.get", fake_get)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 5,
    }
    sample_one_collection(coll, n_bins=1)
    assert captured["params"]["sort_key"] == "revision_date"
    assert captured["params"]["page_size"] == 1


def test_calls_hits_when_count_missing(monkeypatch):
    hits_calls: list[str] = []

    def fake_hits(concept_id):
        hits_calls.append(concept_id)
        return 42

    fetch_offsets: list[int] = []

    def fake_fetch(concept_id, offset):
        fetch_offsets.append(offset)
        return _fake_granule(f"G{offset}")

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample._hits", fake_hits)
    monkeypatch.setattr("nasa_virtual_zarr_survey.sample._fetch_at_offset", fake_fetch)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": None,
    }
    rows = sample_one_collection(coll, n_bins=5)
    assert hits_calls == ["C1"]
    assert fetch_offsets == [0, 8, 16, 25, 33]  # i * 42 // 5 for i in 0..4
    assert all(r["n_total_at_sample"] == 42 for r in rows)


@pytest.mark.parametrize(
    "empty_offsets, expected_bins, expect_warning",
    [
        ({40}, {0, 1, 2, 3, 4}, False),  # bin 2 (offset 40) empty, retry at 39 succeeds
        ({40, 39}, {0, 1, 3, 4}, True),  # both fail, bin 2 dropped, warn
        ({0}, {0, 1, 2, 3, 4}, False),  # bin 0 boundary: retries at +1
    ],
)
def test_retry_on_empty_bin(
    monkeypatch, caplog, empty_offsets, expected_bins, expect_warning
):
    def fake_fetch(concept_id, offset):
        if offset in empty_offsets:
            return None
        return _fake_granule(f"G{offset}")

    monkeypatch.setattr("nasa_virtual_zarr_survey.sample._fetch_at_offset", fake_fetch)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 100,
    }
    with caplog.at_level("WARNING", logger="nasa_virtual_zarr_survey.sample"):
        rows = sample_one_collection(coll, n_bins=5)

    assert {r["stratification_bin"] for r in rows} == expected_bins
    has_warning = any("after retry" in rec.message for rec in caplog.records)
    assert has_warning == expect_warning
