from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from vzc.cmr._sample import (
    _granule_format,
    _reclassify_collection,
    sample,
    sample_one_collection,
)
from vzc.state._io import (
    CollectionRow,
    load_state,
    save_state,
)
from tests.conftest import make_collection, make_state, make_fake_granule


def test_run_sample_persists_granules(tmp_state_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_state_path.parent.parent)
    state = make_state(
        collections=[
            make_collection(
                "C1",
                num_granules=10,
                time_start=datetime(2020, 1, 1),
                time_end=datetime(2024, 1, 1),
            )
        ]
    )
    save_state(state, tmp_state_path)

    counter = iter(range(100))
    monkeypatch.setattr(
        "vzc.cmr._sample._fetch_with_retry",
        lambda concept_id, offset, *, sort_key, bin_index: make_fake_granule(
            f"G{next(counter)}"
        ),
    )

    n = sample(n_bins=3)
    assert n == 3

    state2 = load_state(tmp_state_path)
    pairs = sorted(
        (g.collection_concept_id, g.stratification_bin) for g in state2.granules
    )
    assert pairs == [("C1", 0), ("C1", 1), ("C1", 2)]


def test_sample_one_collection_records_both_urls(monkeypatch):
    captured_accesses: list[str] = []

    def url_for(access: str) -> list[str]:
        captured_accesses.append(access)
        if access == "direct":
            return ["s3://b/G1.nc"]
        return ["https://ex/G1.nc"]

    monkeypatch.setattr(
        "vzc.cmr._sample._fetch_with_retry",
        lambda concept_id, offset, *, sort_key, bin_index: make_fake_granule(
            "G1", urls=url_for
        ),
    )

    coll = {"concept_id": "C1", "time_start": None, "time_end": None, "num_granules": 1}
    gs = sample_one_collection(coll, n_bins=1)
    assert gs[0].s3_url == "s3://b/G1.nc"
    assert gs[0].https_url == "https://ex/G1.nc"
    assert "direct" in captured_accesses
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
# _reclassify_collection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "probed_format, expected_resolved, expected_state",
    [
        ("NetCDF-4", None, ("NetCDF4", "NetCDF-4", None)),
        ("PDF", "non_array_format", (None, "PDF", "non_array_format")),
        (None, "format_unknown", (None, None, "format_unknown")),
    ],
    ids=["array", "non_array", "still_unknown"],
)
def test_reclassify_collection(
    probed_format: str | None,
    expected_resolved: str | None,
    expected_state: tuple,
):
    coll = CollectionRow(
        concept_id="C1",
        format_family=None,
        format_declared=None,
        skip_reason="format_unknown",
    )
    resolved = _reclassify_collection(coll, probed_format)
    assert resolved == expected_resolved
    assert (
        coll.format_family,
        coll.format_declared,
        coll.skip_reason,
    ) == expected_state


# ---------------------------------------------------------------------------
# sample re-classification path
# ---------------------------------------------------------------------------


def test_run_sample_reclassifies_format_unknown(tmp_state_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_state_path.parent.parent)
    state = make_state(
        collections=[
            make_collection(
                "C_UNKNOWN",
                short_name="shortname",
                format_family=None,
                format_declared=None,
                num_granules=5,
                time_start=datetime(2020, 1, 1),
                time_end=datetime(2024, 1, 1),
                skip_reason="format_unknown",
            )
        ]
    )
    save_state(state, tmp_state_path)

    counter = iter(range(100))
    fmt_umm = {
        "DataGranule": {"ArchiveAndDistributionInformation": [{"Format": "NetCDF-4"}]}
    }
    monkeypatch.setattr(
        "vzc.cmr._sample.earthaccess.search_data",
        lambda **_: [make_fake_granule(f"G{next(counter)}", umm=fmt_umm)],
    )
    monkeypatch.setattr(
        "vzc.cmr._sample._fetch_with_retry",
        lambda concept_id, offset, *, sort_key, bin_index: make_fake_granule(
            f"G{next(counter)}", umm=fmt_umm
        ),
    )

    n = sample(n_bins=3)
    assert n == 3

    state2 = load_state(tmp_state_path)
    coll = state2.collection("C_UNKNOWN")
    assert coll is not None
    assert (coll.format_family, coll.format_declared, coll.skip_reason) == (
        "NetCDF4",
        "NetCDF-4",
        None,
    )


def test_run_sample_skips_unresolvable_format_unknown(
    tmp_state_path: Path, monkeypatch
):
    """If probe yields a non-array format, collection is marked non_array_format and not sampled."""
    monkeypatch.chdir(tmp_state_path.parent.parent)
    state = make_state(
        collections=[
            make_collection(
                "C_PDF",
                short_name="n",
                format_family=None,
                format_declared=None,
                num_granules=5,
                time_start=datetime(2020, 1, 1),
                time_end=datetime(2024, 1, 1),
                skip_reason="format_unknown",
            )
        ]
    )
    save_state(state, tmp_state_path)

    pdf_umm = {
        "DataGranule": {"ArchiveAndDistributionInformation": [{"Format": "PDF"}]}
    }
    monkeypatch.setattr(
        "vzc.cmr._sample.earthaccess.search_data",
        lambda **_: [
            make_fake_granule("GP", umm=pdf_umm, urls=["s3://b/whatever.pdf"])
        ],
    )

    n = sample(n_bins=3)
    assert n == 0

    state2 = load_state(tmp_state_path)
    coll = state2.collection("C_PDF")
    assert coll is not None
    assert (coll.format_family, coll.format_declared, coll.skip_reason) == (
        None,
        "PDF",
        "non_array_format",
    )


def test_sample_one_collection_captures_umm_json(monkeypatch):
    """Each granule row should carry the full ``{meta, umm}`` dict."""
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
        "vzc.cmr._sample._fetch_with_retry",
        lambda concept_id, offset, *, sort_key, bin_index: fake,
    )
    coll = {
        "concept_id": "C1-PODAAC",
        "time_start": datetime(2020, 1, 1, tzinfo=timezone.utc),
        "time_end": datetime(2020, 1, 2, tzinfo=timezone.utc),
        "num_granules": 1,
    }
    rows = sample_one_collection(coll, n_bins=1)
    assert len(rows) == 1
    assert rows[0].umm_json == {
        "meta": {"concept-id": "G1-PODAAC"},
        "umm": granule_umm,
    }


def test_run_sample_round_trips_granule_umm_json(tmp_state_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_state_path.parent.parent)
    state = make_state(
        collections=[
            make_collection(
                "C1",
                short_name="short",
                time_start=datetime(2020, 1, 1),
                time_end=datetime(2024, 1, 1),
            )
        ]
    )
    save_state(state, tmp_state_path)

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
        "vzc.cmr._sample._fetch_with_retry",
        lambda concept_id, offset, *, sort_key, bin_index: fake,
    )

    n = sample(n_bins=1)
    assert n == 1

    state2 = load_state(tmp_state_path)
    g = state2.granules_for("C1")[0]
    assert g.umm_json["umm"]["GranuleUR"] == "FOO.nc"


def test_run_sample_skips_already_sampled_collection(tmp_state_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_state_path.parent.parent)
    """Collections that already have granule rows are skipped."""
    state = make_state(
        collections=[make_collection("C1")],
    )
    # Pre-populate with a granule.
    from tests.conftest import make_granule

    state.granules.append(
        make_granule(
            "C1", "G0", s3_url="s3://b/0.nc", https_url="https://x/0.nc", size_bytes=100
        )
    )
    save_state(state, tmp_state_path)

    called = {"n": 0}

    def fake_search_data(**_):
        called["n"] += 1
        raise AssertionError("should not search when granules already exist")

    monkeypatch.setattr("vzc.cmr._sample.earthaccess.search_data", fake_search_data)

    n = sample(n_bins=2)
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
        "vzc.cmr._sample._fetch_with_retry",
        lambda concept_id, offset, *, sort_key, bin_index: make_fake_granule(
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
    rows = sample_one_collection(coll, n_bins=1)
    assert rows[0].s3_url == "s3://b/G1.h5"
    assert rows[0].https_url == "https://x/G1.h5"
    assert rows[0].dmrpp_granule_url == "https://x/G1.h5.dmrpp"


def test_sample_one_collection_dmrpp_url_none_when_no_opendap(monkeypatch):
    monkeypatch.setattr(
        "vzc.cmr._sample._fetch_with_retry",
        lambda concept_id, offset, *, sort_key, bin_index: make_fake_granule(
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
    assert sample_one_collection(coll, n_bins=1)[0].dmrpp_granule_url is None


# ---------------------------------------------------------------------------
# Stratification + offset retry tests
# ---------------------------------------------------------------------------


def _fake_data_granule(concept_id: str):
    from earthaccess.results import DataGranule

    return DataGranule(
        {"meta": {"concept-id": concept_id}, "umm": {"RelatedUrls": []}},
        cloud_hosted=False,
    )


def test_hits_reads_cmr_hits_header(monkeypatch):
    from vzc.cmr._sample import _hits

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

    monkeypatch.setattr("vzc.cmr._sample.requests.get", fake_get)

    n = _hits("C1234-PROV")
    assert n == 12345
    assert captured["url"].endswith("/search/granules.umm_json")
    assert captured["params"]["collection_concept_id"] == "C1234-PROV"
    assert captured["params"]["page_size"] == 0


def test_fetch_at_offset_returns_data_granule(monkeypatch):
    from earthaccess.results import DataGranule

    from vzc.cmr._sample import _fetch_at_offset

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

    monkeypatch.setattr("vzc.cmr._sample.requests.get", fake_get)

    g = _fetch_at_offset("C1", offset=200)
    assert isinstance(g, DataGranule)
    assert g["meta"]["concept-id"] == "G1"
    assert captured["params"]["collection_concept_id"] == "C1"
    assert captured["params"]["sort_key"] == "revision_date"
    assert captured["params"]["page_size"] == 1
    assert captured["params"]["page_num"] == 201  # 1-indexed


def test_fetch_at_offset_returns_none_for_empty_response(monkeypatch):
    from vzc.cmr._sample import _fetch_at_offset

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"items": []}

    monkeypatch.setattr(
        "vzc.cmr._sample.requests.get",
        lambda url, params=None, timeout=None: FakeResponse(),
    )
    assert _fetch_at_offset("C1", offset=999_999) is None


@pytest.mark.parametrize(
    "num_granules, n_bins, expected_calls",
    [
        (
            1000,
            5,
            [
                (0, "revision_date", 0),
                (1, "-revision_date", 750),
                (2, "-revision_date", 500),
                (3, "-revision_date", 250),
                (4, "-revision_date", 0),
            ],
        ),
        (
            3,
            5,
            [
                (0, "revision_date", 0),
                (1, "revision_date", 1),
                (2, "revision_date", 2),
            ],
        ),
        (10, 1, [(0, "revision_date", 0)]),
        (
            2_000_000,
            5,
            [
                (0, "revision_date", 0),
                (1, "-revision_date", 750_000),
                (2, "-revision_date", 500_000),
                (3, "-revision_date", 250_000),
                (4, "-revision_date", 0),
            ],
        ),
    ],
)
def test_stratifies(monkeypatch, num_granules, n_bins, expected_calls):
    captured: list[tuple[int, str, int]] = []

    def fake_fetch_with_retry(concept_id, offset, *, sort_key, bin_index):
        captured.append((bin_index, sort_key, offset))
        return _fake_data_granule(f"G_{sort_key}_{offset}")

    monkeypatch.setattr("vzc.cmr._sample._fetch_with_retry", fake_fetch_with_retry)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": num_granules,
    }
    rows = sample_one_collection(coll, n_bins=n_bins)
    assert captured == expected_calls
    assert len(rows) == len(expected_calls)
    assert all(r.n_total_at_sample == num_granules for r in rows)
    assert [r.stratification_bin for r in rows] == [c[0] for c in expected_calls]


def test_stratifies_uses_both_sort_directions(monkeypatch):
    captured: list[dict] = []

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "items": [{"meta": {"concept-id": "G0"}, "umm": {"RelatedUrls": []}}]
            }

    def fake_get(url, params=None, timeout=None):
        captured.append(dict(params))
        return FakeResponse()

    monkeypatch.setattr("vzc.cmr._sample.requests.get", fake_get)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 100,
    }
    sample_one_collection(coll, n_bins=5)

    sort_keys = [c["sort_key"] for c in captured]
    assert sort_keys[0] == "revision_date"
    assert sort_keys[1:] == ["-revision_date"] * 4
    assert all(c["page_size"] == 1 for c in captured)


def test_calls_hits_when_count_missing(monkeypatch):
    hits_calls: list[str] = []

    def fake_hits(concept_id):
        hits_calls.append(concept_id)
        return 42

    captured: list[tuple[int, str, int]] = []

    def fake_fetch_with_retry(concept_id, offset, *, sort_key, bin_index):
        captured.append((bin_index, sort_key, offset))
        return _fake_data_granule(f"G_{offset}")

    monkeypatch.setattr("vzc.cmr._sample._hits", fake_hits)
    monkeypatch.setattr("vzc.cmr._sample._fetch_with_retry", fake_fetch_with_retry)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": None,
    }
    rows = sample_one_collection(coll, n_bins=5)
    assert hits_calls == ["C1"]
    assert captured == [
        (0, "revision_date", 0),
        (1, "-revision_date", 31),
        (2, "-revision_date", 21),
        (3, "-revision_date", 10),
        (4, "-revision_date", 0),
    ]
    assert all(r.n_total_at_sample == 42 for r in rows)


@pytest.mark.parametrize(
    "empty_calls, expected_bins, expect_warning",
    [
        ({("-revision_date", 50)}, {0, 1, 2, 3, 4}, False),
        ({("-revision_date", 50), ("-revision_date", 49)}, {0, 1, 3, 4}, True),
        ({("-revision_date", 0)}, {0, 1, 2, 3, 4}, False),
    ],
)
def test_retry_on_empty_bin(
    monkeypatch, caplog, empty_calls, expected_bins, expect_warning
):
    def fake_fetch(concept_id, offset, *, sort_key="revision_date"):
        if (sort_key, offset) in empty_calls:
            return None
        return _fake_data_granule(f"G_{sort_key}_{offset}")

    monkeypatch.setattr("vzc.cmr._sample._fetch_at_offset", fake_fetch)

    coll = {
        "concept_id": "C1",
        "time_start": None,
        "time_end": None,
        "num_granules": 100,
    }
    with caplog.at_level("WARNING", logger="vzc.cmr._sample"):
        rows = sample_one_collection(coll, n_bins=5)

    assert {r.stratification_bin for r in rows} == expected_bins
    has_warning = any("after retry" in rec.message for rec in caplog.records)
    assert has_warning == expect_warning
