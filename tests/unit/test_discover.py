from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.discover import (
    collection_row_from_umm,
    persist_collections,
    run_discover,
)


def _build_umm(
    concept_id: str = "C-PODAAC",
    *,
    provider: str = "PODAAC",
    file_dist_format: str | None = "NetCDF-4",
    file_arch_format: str | None = None,
    processing_level: str | None = "L3",
    with_temporal: bool = True,
) -> dict[str, Any]:
    """Build a minimal UMM-C dict for ``collection_row_from_umm`` tests.

    Set ``file_dist_format`` to ``None`` to omit ``FileDistributionInformation``
    (forcing the FileArchiveInformation fallback or the unknown-format path).
    Set ``processing_level`` to ``None`` to omit ProcessingLevel entirely.
    """
    archive: dict[str, Any] = {}
    if file_dist_format is not None:
        archive["FileDistributionInformation"] = [{"Format": file_dist_format}]
    if file_arch_format is not None:
        archive["FileArchiveInformation"] = [{"Format": file_arch_format}]

    umm_inner: dict[str, Any] = {
        "ShortName": "FOO",
        "Version": "1",
        "DataCenters": [{"ShortName": provider}],
        "ArchiveAndDistributionInformation": archive,
    }
    if processing_level is not None:
        umm_inner["ProcessingLevel"] = {"Id": processing_level}
    if with_temporal:
        umm_inner["TemporalExtents"] = [
            {
                "RangeDateTimes": [
                    {
                        "BeginningDateTime": "2020-01-01T00:00:00Z",
                        "EndingDateTime": "2024-01-01T00:00:00Z",
                    }
                ]
            }
        ]

    return {
        "meta": {"concept-id": concept_id, "provider-id": provider},
        "umm": umm_inner,
    }


def test_collection_row_from_umm_extracts_basic_fields():
    """Top-level identity, format classification, and parsed temporal extent."""
    umm = _build_umm("C123-PODAAC", file_dist_format="NetCDF-4")
    row = collection_row_from_umm(umm)
    assert row["concept_id"] == "C123-PODAAC"
    assert row["short_name"] == "FOO"
    assert row["version"] == "1"
    assert row["daac"] == "PODAAC"
    assert row["format_family"] == "NetCDF4"
    assert row["skip_reason"] is None
    assert isinstance(row["time_start"], datetime)


def test_collection_row_from_umm_includes_full_blob():
    umm = _build_umm("C-BLOB-PODAAC", file_dist_format="NetCDF-4")
    row = collection_row_from_umm(umm)
    assert row["umm_json"] == umm
    # The blob is the full top-level dict, including meta, so version info
    # travels with it and no separate version column is needed.
    assert row["umm_json"]["meta"]["concept-id"] == "C-BLOB-PODAAC"
    assert row["umm_json"]["umm"]["ShortName"] == "FOO"


@pytest.mark.parametrize(
    "umm_kwargs, expected_format_family, expected_format_declared, expected_skip_reason",
    [
        # Happy path: declared NetCDF-4 in FileDistributionInformation classifies as NetCDF4.
        (
            dict(file_dist_format="NetCDF-4"),
            "NetCDF4",
            "NetCDF-4",
            None,
        ),
        # Non-array format (PDF) classifies as non_array_format.
        (
            dict(file_dist_format="PDF"),
            None,
            "PDF",
            "non_array_format",
        ),
        # FileDistributionInformation absent: fall back to FileArchiveInformation.
        (
            dict(file_dist_format=None, file_arch_format="NetCDF-4"),
            "NetCDF4",
            "NetCDF-4",
            None,
        ),
        # No format info anywhere → format_unknown.
        (
            dict(file_dist_format=None),
            None,
            None,
            "format_unknown",
        ),
        # Processing level does not gate sampling: an L1B array-like granule
        # still classifies as array-like, ready to be sampled.
        (
            dict(file_dist_format="NetCDF-4", processing_level="L1B"),
            "NetCDF4",
            "NetCDF-4",
            None,
        ),
        # L0 with no declared format still gets format_unknown (not processing_level).
        (
            dict(file_dist_format=None, processing_level="0"),
            None,
            None,
            "format_unknown",
        ),
        # Non-array format stays non_array even with no ProcessingLevel block.
        (
            dict(file_dist_format="PDF", processing_level=None),
            None,
            "PDF",
            "non_array_format",
        ),
    ],
)
def test_collection_row_from_umm_classification(
    umm_kwargs: dict[str, Any],
    expected_format_family: str | None,
    expected_format_declared: str | None,
    expected_skip_reason: str | None,
):
    row = collection_row_from_umm(_build_umm(**umm_kwargs))
    assert row["format_family"] == expected_format_family
    assert row["format_declared"] == expected_format_declared
    assert row["skip_reason"] == expected_skip_reason


def test_persist_collections_round_trips_umm_json(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    persist_collections(con, [_build_umm("C-RT-PODAAC", file_dist_format="NetCDF-4")])
    short_name = con.execute(
        "SELECT json_extract(umm_json, '$.umm.ShortName') FROM collections "
        "WHERE concept_id = 'C-RT-PODAAC'"
    ).fetchone()[0]
    # DuckDB returns json_extract results as quoted JSON strings.
    assert short_name == '"FOO"'


def test_persist_collections_upserts(tmp_db_path: Path):
    con = connect(tmp_db_path)
    init_schema(con)
    umm_rows = [
        _build_umm("C1-PODAAC", file_dist_format="NetCDF-4"),
        _build_umm("C2-PODAAC", file_dist_format="PDF"),
    ]
    persist_collections(con, umm_rows)
    n = con.execute("SELECT count(*) FROM collections").fetchone()[0]
    assert n == 2
    # Re-run with the same data: still 2
    persist_collections(con, umm_rows)
    n = con.execute("SELECT count(*) FROM collections").fetchone()[0]
    assert n == 2


def test_run_discover_uses_earthaccess(tmp_db_path: Path, monkeypatch):
    fake_results = [
        _build_umm("C1-PODAAC", file_dist_format="NetCDF-4"),
        _build_umm("C2-PODAAC", file_dist_format="PDF"),
    ]

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


def test_run_discover_top_per_provider_hydrates_ids(tmp_db_path: Path, monkeypatch):
    # Mock the popularity module's entry point used by discover.
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.popularity.all_top_collection_ids",
        lambda providers, num_per_provider=100: [
            ("C1-PODAAC", 100),
            ("C2-PODAAC", 50),
        ],
    )

    class FakeColl:
        def __init__(self, concept_id):
            self.concept_id = concept_id
            self.render_dict = _build_umm(concept_id, file_dist_format="NetCDF-4")

    captured: list = []

    def fake_search(**kwargs):
        captured.append(kwargs)
        return [FakeColl(cid) for cid in kwargs["concept_id"]]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.discover.earthaccess.search_datasets", fake_search
    )
    run_discover(tmp_db_path, top_per_provider=2)

    con = connect(tmp_db_path)
    init_schema(con)
    ids = sorted(
        r[0] for r in con.execute("SELECT concept_id FROM collections").fetchall()
    )
    assert ids == ["C1-PODAAC", "C2-PODAAC"]
    # Verify earthaccess was called with concept_id= (not cloud_hosted= mode)
    assert captured[0]["concept_id"] == ["C1-PODAAC", "C2-PODAAC"]
    assert "cloud_hosted" not in captured[0]
