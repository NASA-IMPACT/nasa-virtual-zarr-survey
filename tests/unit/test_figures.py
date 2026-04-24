from __future__ import annotations

from pathlib import Path

from nasa_virtual_zarr_survey.cubability import CubabilityResult, CubabilityVerdict
from nasa_virtual_zarr_survey.figures import (
    generate_all,
    generate_funnel,
    generate_group_bars,
    generate_heatmap,
    generate_sankey,
    generate_taxonomy,
)


def _verdict(cid: str, daac: str, fmt: str, pv: str, dv: str) -> dict:
    return {
        "concept_id": cid,
        "daac": daac,
        "format_family": fmt,
        "skip_reason": None,
        "stratified": True,
        "parse_verdict": pv,
        "dataset_verdict": dv,
    }


def _assert_both(stem: Path) -> None:
    """Both .png and .html must exist and be non-empty."""
    png = stem.with_suffix(".png")
    html = stem.with_suffix(".html")
    assert png.exists() and png.stat().st_size > 0, f"Missing or empty: {png}"
    assert html.exists() and html.stat().st_size > 0, f"Missing or empty: {html}"


def test_generate_funnel_creates_file(tmp_path: Path):
    verdicts = [
        _verdict("C1", "PODAAC", "NetCDF4", "all_pass", "all_pass"),
        _verdict("C2", "PODAAC", "NetCDF4", "all_pass", "all_fail"),
        _verdict("C3", "NSIDC", "HDF5", "all_fail", "not_attempted"),
    ]
    cube = {"C1": CubabilityResult(CubabilityVerdict.FEASIBLE)}
    stem = tmp_path / "funnel"
    generate_funnel(verdicts, cube, stem)
    _assert_both(stem)


def test_generate_taxonomy_handles_empty(tmp_path: Path):
    stem = tmp_path / "tax"
    generate_taxonomy({}, "Empty phase taxonomy", stem)
    _assert_both(stem)


def test_generate_taxonomy_with_data(tmp_path: Path):
    stem = tmp_path / "tax"
    generate_taxonomy({"TIMEOUT": (22, 8), "NO_PARSER": (9, 3)}, "Parse taxonomy", stem)
    _assert_both(stem)


def test_generate_group_bars_by_daac(tmp_path: Path):
    verdicts = [
        _verdict("C1", "PODAAC", "NetCDF4", "all_pass", "all_pass"),
        _verdict("C2", "PODAAC", "NetCDF4", "all_fail", "not_attempted"),
        _verdict("C3", "NSIDC", "HDF5", "all_pass", "all_pass"),
    ]
    cube = {
        "C1": CubabilityResult(CubabilityVerdict.FEASIBLE),
        "C3": CubabilityResult(CubabilityVerdict.INCONCLUSIVE),
    }
    stem = tmp_path / "by_daac"
    generate_group_bars(verdicts, cube, "daac", "Pass rate by DAAC", stem)
    _assert_both(stem)


def test_generate_heatmap(tmp_path: Path):
    verdicts = [
        _verdict("C1", "PODAAC", "NetCDF4", "all_pass", "all_pass"),
        _verdict("C2", "PODAAC", "NetCDF4", "partial_pass", "all_fail"),
        _verdict("C3", "NSIDC", "HDF5", "all_fail", "not_attempted"),
    ]
    cube = {"C1": CubabilityResult(CubabilityVerdict.FEASIBLE)}
    stem = tmp_path / "collections"
    generate_heatmap(verdicts, cube, stem)
    _assert_both(stem)


def test_generate_heatmap_empty(tmp_path: Path):
    stem = tmp_path / "collections"
    generate_heatmap([], {}, stem)
    _assert_both(stem)


def test_generate_sankey(tmp_path: Path):
    verdicts = [
        _verdict("C1", "PODAAC", "NetCDF4", "all_pass", "all_pass"),
        _verdict("C2", "PODAAC", "NetCDF4", "all_pass", "all_fail"),
        _verdict("C3", "NSIDC", "HDF5", "all_fail", "not_attempted"),
    ]
    cube = {"C1": CubabilityResult(CubabilityVerdict.FEASIBLE)}
    stem = tmp_path / "sankey"
    generate_sankey(verdicts, cube, stem)
    _assert_both(stem)


def test_generate_sankey_empty(tmp_path: Path):
    stem = tmp_path / "sankey"
    generate_sankey([], {}, stem)
    _assert_both(stem)


def test_generate_all_creates_all_files(tmp_path: Path):
    verdicts = [
        _verdict("C1", "PODAAC", "NetCDF4", "all_pass", "all_pass"),
        _verdict("C2", "NSIDC", "HDF5", "all_fail", "not_attempted"),
    ]
    cube = {"C1": CubabilityResult(CubabilityVerdict.FEASIBLE)}
    out_dir = tmp_path / "figures"
    stems = generate_all(
        verdicts=verdicts,
        cube_results=cube,
        parse_tax={"TIMEOUT": (5, 2)},
        dataset_tax={},
        out_dir=out_dir,
    )
    assert set(stems.keys()) == {
        "sankey",
        "funnel",
        "taxonomy_parse",
        "taxonomy_dataset",
        "by_daac",
        "by_format",
        "collections",
    }
    for stem in stems.values():
        _assert_both(stem)
