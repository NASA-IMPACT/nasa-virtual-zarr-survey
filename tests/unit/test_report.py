from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from vzc.pipeline._cubability import fingerprint_to_json
from vzc.render import collection_verdicts
from vzc.render._orchestrate import _run_render
from vzc.state._io import SCHEMA_VERSION, load_state, save_state
from tests.conftest import make_collection, make_state


_RESULT_SCHEMA = pa.schema(
    [
        ("collection_concept_id", pa.string()),
        ("granule_concept_id", pa.string()),
        ("daac", pa.string()),
        ("format_family", pa.string()),
        ("parser", pa.string()),
        ("attempted_at", pa.timestamp("us", tz="UTC")),
        ("parse_success", pa.bool_()),
        ("parse_error_type", pa.string()),
        ("parse_error_message", pa.string()),
        ("parse_error_traceback", pa.string()),
        ("parse_duration_s", pa.float64()),
        ("dataset_success", pa.bool_()),
        ("dataset_error_type", pa.string()),
        ("dataset_error_message", pa.string()),
        ("dataset_error_traceback", pa.string()),
        ("dataset_duration_s", pa.float64()),
        ("datatree_success", pa.bool_()),
        ("datatree_error_type", pa.string()),
        ("datatree_error_message", pa.string()),
        ("datatree_error_traceback", pa.string()),
        ("datatree_duration_s", pa.float64()),
        ("success", pa.bool_()),
        ("override_applied", pa.bool_()),
        ("timed_out", pa.bool_()),
        ("timed_out_phase", pa.string()),
        ("duration_s", pa.float64()),
        ("fingerprint", pa.string()),
    ]
)


def _write_results(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = {f.name: [r.get(f.name) for r in rows] for f in _RESULT_SCHEMA}
    pq.write_table(pa.table(cols, schema=_RESULT_SCHEMA), path)


def _row(
    cid: str,
    gid: str,
    *,
    parse_success: bool = True,
    dataset_success: bool | None = True,
    datatree_success: bool | None = None,
    parse_error_type: str | None = None,
    parse_error_message: str | None = None,
    dataset_error_type: str | None = None,
    dataset_error_message: str | None = None,
    datatree_error_type: str | None = None,
    datatree_error_message: str | None = None,
    fingerprint: str | None = None,
    now: datetime | None = None,
) -> dict:
    if now is None:
        now = datetime.now(timezone.utc)
    return {
        "collection_concept_id": cid,
        "granule_concept_id": gid,
        "daac": "PODAAC",
        "format_family": "NetCDF4",
        "parser": "HDFParser",
        "attempted_at": now,
        "parse_success": parse_success,
        "parse_error_type": parse_error_type,
        "parse_error_message": parse_error_message,
        "parse_error_traceback": None,
        "parse_duration_s": 0.1,
        "dataset_success": dataset_success,
        "dataset_error_type": dataset_error_type,
        "dataset_error_message": dataset_error_message,
        "dataset_error_traceback": None,
        "dataset_duration_s": 0.1 if dataset_success is not None else 0.0,
        "datatree_success": datatree_success,
        "datatree_error_type": datatree_error_type,
        "datatree_error_message": datatree_error_message,
        "datatree_error_traceback": None,
        "datatree_duration_s": 0.1 if datatree_success is not None else 0.0,
        "success": bool(
            parse_success and (dataset_success is True or datatree_success is True)
        ),
        "override_applied": False,
        "timed_out": False,
        "timed_out_phase": None,
        "duration_s": 0.2,
        "fingerprint": fingerprint,
    }


def _seed(
    state_path: Path, *collections, run_meta: dict[str, str] | None = None
) -> None:
    save_state(make_state(collections=list(collections), run_meta=run_meta), state_path)


def test_datatree_verdict_independent_of_dataset_verdict(
    tmp_state_path, tmp_results_dir
):
    _seed(tmp_state_path, make_collection("CTREE", num_granules=2))

    now = datetime.now(timezone.utc)
    rows = [
        _row(
            "CTREE",
            "G0",
            parse_success=True,
            dataset_success=False,
            datatree_success=True,
            dataset_error_type="ValueError",
            dataset_error_message="conflicting sizes for dimension",
            now=now,
        ),
        _row(
            "CTREE",
            "G1",
            parse_success=True,
            dataset_success=False,
            datatree_success=True,
            dataset_error_type="ValueError",
            dataset_error_message="conflicting sizes for dimension",
            now=now,
        ),
    ]
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", rows)

    verdicts = collection_verdicts(load_state(tmp_state_path), tmp_results_dir)
    by_id = {v["concept_id"]: v for v in verdicts}
    assert by_id["CTREE"]["parse_verdict"] == "all_pass"
    assert by_id["CTREE"]["dataset_verdict"] == "all_fail"
    assert by_id["CTREE"]["datatree_verdict"] == "all_pass"


def test_collection_verdicts_classifies_all_three(tmp_state_path, tmp_results_dir):
    _seed(
        tmp_state_path,
        make_collection("C_ALL", num_granules=5),
        make_collection("C_PART", num_granules=5),
        make_collection("C_NONE", num_granules=5),
        make_collection(
            "C_SKIP",
            format_family=None,
            format_declared="PDF",
            num_granules=5,
            skip_reason="non_array_format",
        ),
    )

    now = datetime.now(timezone.utc)
    rows = []
    for i in range(3):
        rows.append(
            _row("C_ALL", f"G{i}", parse_success=True, dataset_success=True, now=now)
        )
    rows.append(_row("C_PART", "G0", parse_success=True, dataset_success=True, now=now))
    rows.append(
        _row(
            "C_PART",
            "G1",
            parse_success=True,
            dataset_success=False,
            dataset_error_type="ValueError",
            dataset_error_message="codec not found",
            now=now,
        )
    )
    rows.append(
        _row(
            "C_PART",
            "G2",
            parse_success=True,
            dataset_success=False,
            dataset_error_type="ValueError",
            dataset_error_message="codec not found",
            now=now,
        )
    )
    for i in range(3):
        rows.append(
            _row(
                "C_NONE",
                f"G{i}",
                parse_success=False,
                dataset_success=None,
                parse_error_type="PermissionError",
                parse_error_message="403 Forbidden",
                now=now,
            )
        )
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", rows)

    verdicts = collection_verdicts(load_state(tmp_state_path), tmp_results_dir)
    by_id = {v["concept_id"]: v for v in verdicts}

    assert by_id["C_ALL"]["parse_verdict"] == "all_pass"
    assert by_id["C_ALL"]["dataset_verdict"] == "all_pass"
    assert by_id["C_PART"]["parse_verdict"] == "all_pass"
    assert by_id["C_PART"]["dataset_verdict"] == "partial_pass"
    assert by_id["C_NONE"]["parse_verdict"] == "all_fail"
    assert by_id["C_NONE"]["dataset_verdict"] == "not_attempted"
    assert by_id["C_SKIP"]["parse_verdict"] == "skipped"
    assert by_id["C_SKIP"]["dataset_verdict"] == "skipped"


def test_parse_fail_means_dataset_not_attempted(tmp_state_path, tmp_results_dir):
    _seed(
        tmp_state_path,
        make_collection(
            "C_FAIL", format_family="HDF4", format_declared="HDF", num_granules=2
        ),
    )

    now = datetime.now(timezone.utc)
    rows = [
        _row(
            "C_FAIL",
            "G0",
            parse_success=False,
            dataset_success=None,
            parse_error_type="NoParserAvailable",
            parse_error_message="no parser for HDF4",
            now=now,
        ),
        _row(
            "C_FAIL",
            "G1",
            parse_success=False,
            dataset_success=None,
            parse_error_type="NoParserAvailable",
            parse_error_message="no parser for HDF4",
            now=now,
        ),
    ]
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", rows)

    verdicts = collection_verdicts(load_state(tmp_state_path), tmp_results_dir)
    by_id = {v["concept_id"]: v for v in verdicts}
    assert by_id["C_FAIL"]["parse_verdict"] == "all_fail"
    assert by_id["C_FAIL"]["dataset_verdict"] == "not_attempted"


def _make_fp(time_hash: str, time_min: int, time_max: int) -> str:
    fp = {
        "dims": {"time": 10, "lat": 5, "lon": 10},
        "data_vars": {
            "temp": {
                "dtype": "float32",
                "dims": ["time", "lat", "lon"],
                "chunks": [1, 5, 10],
                "fill_value": None,
                "codecs": ["Blosc"],
            }
        },
        "coords": {
            "time": {
                "dtype": "int64",
                "dims": ["time"],
                "shape": [10],
                "values_hash": time_hash,
                "min": time_min,
                "max": time_max,
            },
            "lat": {
                "dtype": "float32",
                "dims": ["lat"],
                "shape": [5],
                "values_hash": "lathash",
                "min": -45.0,
                "max": 45.0,
            },
            "lon": {
                "dtype": "float32",
                "dims": ["lon"],
                "shape": [10],
                "values_hash": "lonhash",
                "min": -90.0,
                "max": 90.0,
            },
        },
    }
    return fingerprint_to_json(fp)


def test_render_report_includes_skipped_by_format_section(
    tmp_state_path, tmp_results_dir, tmp_path
):
    _seed(
        tmp_state_path,
        make_collection("CARR"),
        make_collection(
            "CPDF1",
            format_family=None,
            format_declared="PDF",
            skip_reason="non_array_format",
        ),
        make_collection(
            "CPDF2",
            format_family=None,
            format_declared="PDF",
            skip_reason="non_array_format",
        ),
        make_collection(
            "CGRIB",
            format_family=None,
            format_declared="GRIB",
            skip_reason="non_array_format",
        ),
    )

    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [_row("CARR", "G0")],
    )

    out = tmp_path / "report.md"
    _run_render(
        state=load_state(tmp_state_path), results_dir=tmp_results_dir, out_path=out
    )
    text = out.read_text()
    assert "## Skipped collections by declared format" in text
    assert "| Declared format | Reason | Collections |" in text
    pdf_idx = text.index("| PDF | non_array_format | 2 |")
    grib_idx = text.index("| GRIB | non_array_format | 1 |")
    assert pdf_idx < grib_idx
    assert "| NetCDF-4 |" not in text.split("## Phase 3")[0]


def test_render_report_reports_rescued_by_datatree_count(
    tmp_state_path, tmp_results_dir, tmp_path
):
    _seed(
        tmp_state_path,
        make_collection("CRESCUE", num_granules=2),
        make_collection("CCLEAN"),
    )

    now = datetime.now(timezone.utc)
    rows = [
        _row(
            "CRESCUE",
            "G0",
            parse_success=True,
            dataset_success=False,
            datatree_success=True,
            dataset_error_type="ValueError",
            dataset_error_message="conflicting sizes for dimension 'x'",
            now=now,
        ),
        _row(
            "CRESCUE",
            "G1",
            parse_success=True,
            dataset_success=False,
            datatree_success=True,
            dataset_error_type="ValueError",
            dataset_error_message="conflicting sizes for dimension 'x'",
            now=now,
        ),
        _row("CCLEAN", "G0", parse_success=True, dataset_success=True, now=now),
    ]
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", rows)

    out = tmp_path / "report.md"
    _run_render(
        state=load_state(tmp_state_path), results_dir=tmp_results_dir, out_path=out
    )
    text = out.read_text()
    assert (
        "**Rescued by Phase 4b:** 1 collection(s) that failed Phase 4a "
        "(`CONFLICTING_DIM_SIZES`) succeeded under Phase 4b." in text
    )


def test_render_report_includes_metadata_block(
    tmp_state_path, tmp_results_dir, tmp_path
):
    _seed(
        tmp_state_path,
        make_collection("C1"),
        run_meta={"sampling_mode": "top=200"},
    )

    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [_row("C1", "G1")],
    )

    out = tmp_path / "report.md"
    _run_render(
        state=load_state(tmp_state_path), results_dir=tmp_results_dir, out_path=out
    )
    text = out.read_text()

    header, _, rest = text.partition("## Overview")
    assert "**Generated:**" in header
    assert "**Survey tool:**" in header
    assert "**VirtualiZarr:**" in header
    assert "**Sampling mode:** top=200" in header
    assert "## Phase" not in header
    assert rest


def test_render_report_contains_counts(tmp_state_path, tmp_results_dir, tmp_path):
    _seed(
        tmp_state_path,
        make_collection("C1"),
        make_collection("C2", num_granules=2),
    )

    now = datetime.now(timezone.utc)
    fp1 = _make_fp("hash_a", 0, 9)
    fp2 = _make_fp("hash_b", 10, 19)
    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [
            _row("C1", "G1", fingerprint=fp1, now=now),
            _row("C2", "G2a", fingerprint=fp1, now=now),
            _row("C2", "G2b", fingerprint=fp2, now=now),
        ],
    )

    out = tmp_path / "report.md"
    _run_render(
        state=load_state(tmp_state_path), results_dir=tmp_results_dir, out_path=out
    )
    text = out.read_text()
    assert "Phase 3: Parsability" in text
    assert "Phase 4a: Datasetability" in text
    assert "Phase 4b: Datatreeability" in text
    assert "Phase 5: Cubability" in text
    assert "all_pass" in text
    assert "PODAAC" in text
    assert "NetCDF4" in text
    assert "FEASIBLE" in text
    assert "## Collections" in text
    assert (
        "| concept_id | daac | format | parse | dataset | datatree | cube | top_bucket |"
        in text
    )
    assert "| C1 |" in text
    assert "| C2 |" in text
    assert "Datatree Failure Taxonomy" in text
    assert 'src="figures/funnel.html"' in text
    assert (tmp_path / "figures").is_dir()
    assert (tmp_path / "figures" / "funnel.html").exists()
    assert (tmp_path / "figures" / "funnel.png").exists()


def test_render_report_incompatible_detection(
    tmp_state_path, tmp_results_dir, tmp_path
):
    _seed(tmp_state_path, make_collection("CINC", num_granules=2))

    now = datetime.now(timezone.utc)

    def _fp_incompatible(
        dtype: str, time_hash: str, time_min: str, time_max: str
    ) -> str:
        fp = {
            "dims": {"time": 10, "lat": 5},
            "data_vars": {
                "sst": {
                    "dtype": dtype,
                    "dims": ["time", "lat"],
                    "chunks": [1, 5],
                    "fill_value": None,
                    "codecs": [],
                }
            },
            "coords": {
                "time": {
                    "dtype": "int64",
                    "dims": ["time"],
                    "shape": [10],
                    "values_hash": time_hash,
                    "min": time_min,
                    "max": time_max,
                },
            },
        }
        return fingerprint_to_json(fp)

    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [
            _row(
                "CINC",
                "GA",
                fingerprint=_fp_incompatible("float32", "h1", "0", "9"),
                now=now,
            ),
            _row(
                "CINC",
                "GB",
                fingerprint=_fp_incompatible("float64", "h2", "10", "19"),
                now=now,
            ),
        ],
    )

    out = tmp_path / "report.md"
    _run_render(
        state=load_state(tmp_state_path), results_dir=tmp_results_dir, out_path=out
    )
    text = out.read_text()
    assert "INCOMPATIBLE" in text
    assert "dtype" in text


def test_taxonomy_counts_reports_granule_and_collection_counts(
    tmp_state_path, tmp_results_dir, tmp_path
):
    _seed(
        tmp_state_path,
        make_collection("C1", format_family="HDF4", format_declared="HDF"),
        make_collection("C2", format_family="HDF4", format_declared="HDF"),
    )

    now = datetime.now(timezone.utc)
    rows = []
    for cid in ["C1", "C2"]:
        for i in range(3):
            rows.append(
                _row(
                    cid,
                    f"{cid}-G{i}",
                    parse_success=False,
                    dataset_success=None,
                    parse_error_type="NoParserAvailable",
                    parse_error_message="No VirtualiZarr parser registered for HDF4",
                    now=now,
                )
            )
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", rows)

    out = tmp_path / "report.md"
    _run_render(
        state=load_state(tmp_state_path), results_dir=tmp_results_dir, out_path=out
    )
    text = out.read_text()
    assert "| NO_PARSER | 6 | 2 |" in text
    assert "| Bucket | Granules | Collections |" in text


def test_three_phase_daac_table_format(tmp_state_path, tmp_results_dir, tmp_path):
    _seed(tmp_state_path, make_collection("C1"))

    now = datetime.now(timezone.utc)
    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [_row("C1", "G1", now=now)],
    )

    out = tmp_path / "report.md"
    _run_render(
        state=load_state(tmp_state_path), results_dir=tmp_results_dir, out_path=out
    )
    text = out.read_text()
    assert "By DAAC" in text
    assert "Parsable" in text
    assert "Datasetable" in text
    assert "Datatreeable" in text
    assert "Cubable" in text
    assert "PODAAC" in text


def test_three_phase_table_handles_missing_concept_id_in_cube_results():
    from vzc.render._markdown import _render_three_phase_table
    from vzc.core.types import VerdictRow

    row: VerdictRow = {
        "concept_id": "C_MISSING",
        "daac": "PODAAC",
        "format_family": "NETCDF4",
        "skip_reason": None,
        "processing_level": "L3",
        "parse_verdict": "all_pass",
        "dataset_verdict": "all_pass",
        "datatree_verdict": "all_pass",
        "top_bucket": "",
    }

    lines = _render_three_phase_table([row], {}, "By DAAC", "daac")
    text = "\n".join(lines)
    assert "| PODAAC | 1/1 (100%) | 1/1 (100%) | 1/1 (100%) | 0/1 (0%) |" in text


def test_l2_collection_gets_excluded_by_policy_cubability(
    tmp_state_path, tmp_results_dir
):
    from vzc.pipeline._cubability import CubabilityVerdict
    from vzc.render._aggregate import (
        cubability_results as _cubability_results,
    )

    _seed(
        tmp_state_path,
        make_collection("C_L2", num_granules=2, processing_level="L2"),
        make_collection("C_L3", num_granules=2),
    )

    now = datetime.now(timezone.utc)
    fp = _make_fp("h", 0, 9)
    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [
            _row("C_L2", "G1", fingerprint=fp, now=now),
            _row("C_L2", "G2", fingerprint=fp, now=now),
            _row("C_L3", "G1", fingerprint=fp, now=now),
            _row("C_L3", "G2", fingerprint=fp, now=now),
        ],
    )

    state = load_state(tmp_state_path)
    verdicts = collection_verdicts(state, tmp_results_dir)
    cube_results = _cubability_results(tmp_results_dir, verdicts)

    assert cube_results["C_L2"].verdict == CubabilityVerdict.EXCLUDED_BY_POLICY
    assert "L2" in cube_results["C_L2"].reason
    assert cube_results["C_L3"].verdict != CubabilityVerdict.EXCLUDED_BY_POLICY


def test_export_then_from_data_produces_identical_report(
    tmp_state_path, tmp_results_dir, tmp_path
):
    """Export a JSON digest, then regenerate from it; the two reports must be identical."""
    _seed(
        tmp_state_path,
        make_collection("C1"),
        make_collection("C2", num_granules=2),
    )

    now = datetime.now(timezone.utc)
    fp1 = _make_fp("hash_a", 0, 9)
    fp2 = _make_fp("hash_b", 10, 19)
    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [
            _row("C1", "G1", fingerprint=fp1, now=now),
            _row("C2", "G2a", fingerprint=fp1, now=now),
            _row("C2", "G2b", fingerprint=fp2, now=now),
        ],
    )

    out1 = tmp_path / "index.md"
    digest = tmp_path / "summary.json"
    _run_render(
        state=load_state(tmp_state_path),
        results_dir=tmp_results_dir,
        out_path=out1,
        export_to=digest,
    )
    assert digest.exists(), "export_to did not create the digest file"

    out2 = tmp_path / "index2.md"
    _run_render(
        state=load_state(tmp_state_path),
        results_dir=tmp_results_dir,
        out_path=out2,
        from_data=digest,
    )

    text1 = out1.read_text()
    text2 = out2.read_text()
    assert text1 == text2, (
        "Report regenerated from digest differs from original.\n"
        f"First diff line: {next((left for left, right in zip(text1.splitlines(), text2.splitlines()) if left != right), 'length differs')}"
    )


# === Snapshot / preview / provenance tests ===


def _sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _locked_sample_payload() -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_meta": {"sampling_mode": "top=1"},
        "collections": [
            {
                "concept_id": "C1-T",
                "daac": "X.DAAC",
                "provider": "PODAAC",
                "format_family": "NetCDF4",
                "processing_level": "L4",
                "short_name": "FOO",
                "version": "1.0",
            }
        ],
        "granules": [
            {
                "collection_concept_id": "C1-T",
                "granule_concept_id": "G1-T",
                "s3_url": "s3://b/k1",
                "https_url": "https://h/k1",
                "stratification_bin": 0,
                "n_total_at_sample": 100,
                "size_bytes": 100,
            }
        ],
    }


# Snapshot/provenance functionality (--locked-sample, --export, --snapshot-date,
# --label, --description, --no-render) moved from `report` to `run` in the CLI
# simplification. See tests/unit/test_snapshot.py for coverage of those flows.
