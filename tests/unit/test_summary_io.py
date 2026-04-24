"""Tests for summary_io: roundtrip, schema version guard, and CubabilityResult serialization."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nasa_virtual_zarr_survey.cubability import CubabilityResult, CubabilityVerdict
from nasa_virtual_zarr_survey.summary_io import dump_summary, load_summary
from nasa_virtual_zarr_survey.types import VerdictRow


def _make_verdict(concept_id: str = "C1") -> VerdictRow:
    return VerdictRow(
        concept_id=concept_id,
        daac="PODAAC",
        format_family="NetCDF4",
        skip_reason=None,
        stratified=True,
        parse_verdict="all_pass",
        dataset_verdict="all_pass",
        top_bucket="",
    )


def test_roundtrip_empty(tmp_path: Path) -> None:
    """Dump then load with empty collections produces equal structures."""
    p = tmp_path / "summary.json"
    dump_summary(
        p,
        verdicts=[],
        parse_taxonomy={},
        dataset_taxonomy={},
        cubability_results={},
        other_parse_errors=[],
        other_dataset_errors=[],
        survey_tool_version="0.1.0",
    )
    summary = load_summary(p)
    assert summary.verdicts == []
    assert summary.parse_taxonomy == {}
    assert summary.dataset_taxonomy == {}
    assert summary.cubability_results == {}
    assert summary.other_parse_errors == []
    assert summary.other_dataset_errors == []
    assert summary.survey_tool_version == "0.1.0"


def test_roundtrip_with_data(tmp_path: Path) -> None:
    """All fields survive a dump/load cycle."""
    v1 = _make_verdict("C1")
    v2 = VerdictRow(
        concept_id="C2",
        daac="NSIDC",
        format_family="HDF5",
        skip_reason=None,
        stratified=False,
        parse_verdict="all_fail",
        dataset_verdict="not_attempted",
        top_bucket="NO_PARSER",
    )
    parse_tax: dict[str, tuple[int, int]] = {"NO_PARSER": (6, 2), "OTHER": (1, 1)}
    dataset_tax: dict[str, tuple[int, int]] = {"UNSUPPORTED_CODEC": (3, 1)}
    cube_results = {
        "C1": CubabilityResult(
            verdict=CubabilityVerdict.FEASIBLE, reason="", concat_dim="time"
        ),
        "C2": CubabilityResult(verdict=CubabilityVerdict.NOT_ATTEMPTED, reason=""),
    }
    other_parse: list[tuple[int, str, str]] = [(5, "SomeError", "some message")]
    other_dataset: list[tuple[int, str, str]] = [(2, "OtherError", "other message")]

    p = tmp_path / "summary.json"
    dump_summary(
        p,
        verdicts=[v1, v2],
        parse_taxonomy=parse_tax,
        dataset_taxonomy=dataset_tax,
        cubability_results=cube_results,
        other_parse_errors=other_parse,
        other_dataset_errors=other_dataset,
        survey_tool_version="1.2.3",
    )

    summary = load_summary(p)

    assert len(summary.verdicts) == 2
    by_id = {v["concept_id"]: v for v in summary.verdicts}
    assert by_id["C1"]["daac"] == "PODAAC"
    assert by_id["C1"]["top_bucket"] == ""
    assert by_id["C2"]["top_bucket"] == "NO_PARSER"
    assert by_id["C2"]["parse_verdict"] == "all_fail"

    assert summary.parse_taxonomy == parse_tax
    assert summary.dataset_taxonomy == dataset_tax

    assert summary.cubability_results["C1"].verdict == CubabilityVerdict.FEASIBLE
    assert summary.cubability_results["C1"].concat_dim == "time"
    assert summary.cubability_results["C2"].verdict == CubabilityVerdict.NOT_ATTEMPTED

    assert summary.other_parse_errors == other_parse
    assert summary.other_dataset_errors == other_dataset
    assert summary.survey_tool_version == "1.2.3"


def test_schema_version_mismatch_raises(tmp_path: Path) -> None:
    """load_summary raises ValueError when schema_version doesn't match."""
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"schema_version": 999, "verdicts": []}))
    with pytest.raises(ValueError, match="Unsupported schema_version"):
        load_summary(p)


def test_cubability_result_roundtrip(tmp_path: Path) -> None:
    """CubabilityResult fields (verdict, reason, concat_dim) all survive serialization."""
    cube_results = {
        "feasible": CubabilityResult(
            verdict=CubabilityVerdict.FEASIBLE, reason="", concat_dim="time"
        ),
        "incompatible": CubabilityResult(
            verdict=CubabilityVerdict.INCOMPATIBLE,
            reason="variables differ: ['sst']",
            concat_dim=None,
        ),
        "inconclusive": CubabilityResult(
            verdict=CubabilityVerdict.INCONCLUSIVE,
            reason="fewer than 2 fingerprints",
            concat_dim=None,
        ),
        "not_attempted": CubabilityResult(
            verdict=CubabilityVerdict.NOT_ATTEMPTED, reason=""
        ),
    }
    p = tmp_path / "summary.json"
    dump_summary(
        p,
        verdicts=[],
        parse_taxonomy={},
        dataset_taxonomy={},
        cubability_results=cube_results,
        other_parse_errors=[],
        other_dataset_errors=[],
        survey_tool_version="0.0.1",
    )
    summary = load_summary(p)
    cr = summary.cubability_results
    assert cr["feasible"].verdict == CubabilityVerdict.FEASIBLE
    assert cr["feasible"].concat_dim == "time"
    assert cr["incompatible"].verdict == CubabilityVerdict.INCOMPATIBLE
    assert cr["incompatible"].reason == "variables differ: ['sst']"
    assert cr["incompatible"].concat_dim is None
    assert cr["inconclusive"].verdict == CubabilityVerdict.INCONCLUSIVE
    assert cr["inconclusive"].reason == "fewer than 2 fingerprints"
    assert cr["not_attempted"].verdict == CubabilityVerdict.NOT_ATTEMPTED


def test_dump_creates_parent_dirs(tmp_path: Path) -> None:
    """dump_summary creates intermediate directories automatically."""
    p = tmp_path / "nested" / "deep" / "summary.json"
    dump_summary(
        p,
        verdicts=[],
        parse_taxonomy={},
        dataset_taxonomy={},
        cubability_results={},
        other_parse_errors=[],
        other_dataset_errors=[],
        survey_tool_version="0.0.0",
    )
    assert p.exists()
