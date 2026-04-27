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
        parse_verdict="all_pass",
        dataset_verdict="all_pass",
        datatree_verdict="all_pass",
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
        datatree_taxonomy={},
        cubability_results={},
        other_parse_errors=[],
        other_dataset_errors=[],
        other_datatree_errors=[],
        survey_tool_version="0.1.0",
    )
    summary = load_summary(p)
    assert summary.verdicts == []
    assert summary.parse_taxonomy == {}
    assert summary.dataset_taxonomy == {}
    assert summary.datatree_taxonomy == {}
    assert summary.cubability_results == {}
    assert summary.other_parse_errors == []
    assert summary.other_dataset_errors == []
    assert summary.other_datatree_errors == []
    assert summary.survey_tool_version == "0.1.0"


def test_roundtrip_with_data(tmp_path: Path) -> None:
    """All fields survive a dump/load cycle."""
    v1 = _make_verdict("C1")
    v2 = VerdictRow(
        concept_id="C2",
        daac="NSIDC",
        format_family="HDF5",
        skip_reason=None,
        parse_verdict="all_fail",
        dataset_verdict="not_attempted",
        datatree_verdict="not_attempted",
        top_bucket="NO_PARSER",
    )
    parse_tax: dict[str, tuple[int, int]] = {"NO_PARSER": (6, 2), "OTHER": (1, 1)}
    dataset_tax: dict[str, tuple[int, int]] = {"UNSUPPORTED_CODEC": (3, 1)}
    datatree_tax: dict[str, tuple[int, int]] = {"CONFLICTING_DIM_SIZES": (2, 1)}
    cube_results = {
        "C1": CubabilityResult(
            verdict=CubabilityVerdict.FEASIBLE, reason="", concat_dim="time"
        ),
        "C2": CubabilityResult(verdict=CubabilityVerdict.NOT_ATTEMPTED, reason=""),
    }
    other_parse: list[tuple[int, str, str]] = [(5, "SomeError", "some message")]
    other_dataset: list[tuple[int, str, str]] = [(2, "OtherError", "other message")]
    other_datatree: list[tuple[int, str, str]] = [(1, "TreeError", "tree message")]

    p = tmp_path / "summary.json"
    dump_summary(
        p,
        verdicts=[v1, v2],
        parse_taxonomy=parse_tax,
        dataset_taxonomy=dataset_tax,
        datatree_taxonomy=datatree_tax,
        cubability_results=cube_results,
        other_parse_errors=other_parse,
        other_dataset_errors=other_dataset,
        other_datatree_errors=other_datatree,
        survey_tool_version="1.2.3",
    )

    summary = load_summary(p)

    assert len(summary.verdicts) == 2
    by_id = {v["concept_id"]: v for v in summary.verdicts}
    assert by_id["C1"]["daac"] == "PODAAC"
    assert by_id["C1"]["top_bucket"] == ""
    assert by_id["C1"]["datatree_verdict"] == "all_pass"
    assert by_id["C2"]["top_bucket"] == "NO_PARSER"
    assert by_id["C2"]["parse_verdict"] == "all_fail"
    assert by_id["C2"]["datatree_verdict"] == "not_attempted"

    assert summary.parse_taxonomy == parse_tax
    assert summary.dataset_taxonomy == dataset_tax
    assert summary.datatree_taxonomy == datatree_tax

    assert summary.cubability_results["C1"].verdict == CubabilityVerdict.FEASIBLE
    assert summary.cubability_results["C1"].concat_dim == "time"
    assert summary.cubability_results["C2"].verdict == CubabilityVerdict.NOT_ATTEMPTED

    assert summary.other_parse_errors == other_parse
    assert summary.other_dataset_errors == other_dataset
    assert summary.other_datatree_errors == other_datatree
    assert summary.survey_tool_version == "1.2.3"


def test_schema_version_mismatch_raises(tmp_path: Path) -> None:
    """load_summary raises ValueError when schema_version doesn't match."""
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"schema_version": 999, "verdicts": []}))
    with pytest.raises(ValueError, match="Unsupported schema_version"):
        load_summary(p)


def test_skipped_by_format_roundtrip(tmp_path: Path) -> None:
    """skipped_by_format rows carry example collection short names through dump/load."""
    p = tmp_path / "summary.json"
    skipped = [
        ("PDF", "non_array_format", 4, ["MOD09GA", "MOD13Q1"]),
        ("(null)", "format_unknown", 2, ["AIRS2RET"]),
        ("CSV", "non_array_format", 1, []),
    ]
    dump_summary(
        p,
        verdicts=[],
        parse_taxonomy={},
        dataset_taxonomy={},
        datatree_taxonomy={},
        cubability_results={},
        other_parse_errors=[],
        other_dataset_errors=[],
        other_datatree_errors=[],
        skipped_by_format=skipped,
        survey_tool_version="1.0.0",
    )
    summary = load_summary(p)
    assert summary.skipped_by_format == skipped


def test_metadata_fields_roundtrip(tmp_path: Path) -> None:
    """Versions, sampling mode, and explicit generated_at survive dump/load."""
    p = tmp_path / "summary.json"
    dump_summary(
        p,
        verdicts=[],
        parse_taxonomy={},
        dataset_taxonomy={},
        datatree_taxonomy={},
        cubability_results={},
        other_parse_errors=[],
        other_dataset_errors=[],
        other_datatree_errors=[],
        survey_tool_version="1.2.3",
        virtualizarr_version="1.3.0",
        zarr_version="3.0.8",
        xarray_version="2025.1.0",
        sampling_mode="top=200",
        generated_at="2026-04-23T15:42:00+00:00",
    )
    summary = load_summary(p)
    assert summary.generated_at == "2026-04-23T15:42:00+00:00"
    assert summary.virtualizarr_version == "1.3.0"
    assert summary.zarr_version == "3.0.8"
    assert summary.xarray_version == "2025.1.0"
    assert summary.sampling_mode == "top=200"


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
        datatree_taxonomy={},
        cubability_results=cube_results,
        other_parse_errors=[],
        other_dataset_errors=[],
        other_datatree_errors=[],
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
        datatree_taxonomy={},
        cubability_results={},
        other_parse_errors=[],
        other_dataset_errors=[],
        other_datatree_errors=[],
        survey_tool_version="0.0.0",
    )
    assert p.exists()
