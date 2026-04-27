from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from nasa_virtual_zarr_survey.__main__ import cli
from nasa_virtual_zarr_survey.cubability import fingerprint_to_json
from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.report import collection_verdicts, run_report
from tests.conftest import insert_collection, insert_granule

# New schema matching attempt._SCHEMA
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
        "timed_out": False,
        "timed_out_phase": None,
        "duration_s": 0.2,
        "fingerprint": fingerprint,
    }


def test_datatree_verdict_independent_of_dataset_verdict(tmp_db_path, tmp_results_dir):
    """A collection with dataset_success=False but datatree_success=True has correct verdicts."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "CTREE", num_granules=2)
    con.close()

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

    verdicts = collection_verdicts(tmp_db_path, tmp_results_dir)
    by_id = {v["concept_id"]: v for v in verdicts}
    assert by_id["CTREE"]["parse_verdict"] == "all_pass"
    assert by_id["CTREE"]["dataset_verdict"] == "all_fail"
    assert by_id["CTREE"]["datatree_verdict"] == "all_pass"


def test_collection_verdicts_classifies_all_three(tmp_db_path, tmp_results_dir):
    con = connect(tmp_db_path)
    init_schema(con)
    for cid in ["C_ALL", "C_PART", "C_NONE"]:
        insert_collection(con, cid, num_granules=5)
    insert_collection(
        con,
        "C_SKIP",
        format_family=None,
        format_declared="PDF",
        num_granules=5,
        skip_reason="non_array_format",
    )
    con.close()

    now = datetime.now(timezone.utc)
    rows = []
    # C_ALL: all parse and dataset succeed
    for i in range(3):
        rows.append(
            _row("C_ALL", f"G{i}", parse_success=True, dataset_success=True, now=now)
        )
    # C_PART: first granule fully succeeds, others fail dataset
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
    # C_NONE: all fail to parse
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

    verdicts = collection_verdicts(tmp_db_path, tmp_results_dir)
    by_id = {v["concept_id"]: v for v in verdicts}

    assert by_id["C_ALL"]["parse_verdict"] == "all_pass"
    assert by_id["C_ALL"]["dataset_verdict"] == "all_pass"
    assert by_id["C_PART"]["parse_verdict"] == "all_pass"
    assert by_id["C_PART"]["dataset_verdict"] == "partial_pass"
    assert by_id["C_NONE"]["parse_verdict"] == "all_fail"
    assert by_id["C_NONE"]["dataset_verdict"] == "not_attempted"
    assert by_id["C_SKIP"]["parse_verdict"] == "skipped"
    assert by_id["C_SKIP"]["dataset_verdict"] == "skipped"


def test_parse_fail_means_dataset_not_attempted(tmp_db_path, tmp_results_dir):
    """When parse fails for all granules, dataset should show not_attempted."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(
        con, "C_FAIL", format_family="HDF4", format_declared="HDF", num_granules=2
    )
    con.close()

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

    verdicts = collection_verdicts(tmp_db_path, tmp_results_dir)
    by_id = {v["concept_id"]: v for v in verdicts}
    assert by_id["C_FAIL"]["parse_verdict"] == "all_fail"
    assert by_id["C_FAIL"]["dataset_verdict"] == "not_attempted"


def _make_fp(time_hash: str, time_min: int, time_max: int) -> str:
    """Build a minimal fingerprint JSON with varying time coord."""
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
    tmp_db_path, tmp_results_dir, tmp_path
):
    """The Skipped collections section lists declared formats sorted descending
    by count, drawing from collections.skip_reason + collections.format_declared."""
    con = connect(tmp_db_path)
    init_schema(con)
    # One array-like (no skip), three skipped with two distinct declared formats.
    insert_collection(con, "CARR")
    insert_collection(
        con,
        "CPDF1",
        format_family=None,
        format_declared="PDF",
        skip_reason="non_array_format",
    )
    insert_collection(
        con,
        "CPDF2",
        format_family=None,
        format_declared="PDF",
        skip_reason="non_array_format",
    )
    insert_collection(
        con,
        "CGRIB",
        format_family=None,
        format_declared="GRIB",
        skip_reason="non_array_format",
    )
    con.close()

    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [_row("CARR", "G0")],
    )

    out = tmp_path / "report.md"
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()
    assert "## Skipped collections by declared format" in text
    assert "| Declared format | Reason | Collections |" in text
    # PDF (n=2) must precede GRIB (n=1) — sorted descending.
    pdf_idx = text.index("| PDF | non_array_format | 2 |")
    grib_idx = text.index("| GRIB | non_array_format | 1 |")
    assert pdf_idx < grib_idx
    # Array-like collection must not appear in this section's table.
    assert "| NetCDF-4 |" not in text.split("## Phase 3")[0]


def test_render_report_reports_rescued_by_datatree_count(
    tmp_db_path, tmp_results_dir, tmp_path
):
    """Report's Phase 4b section surfaces the count of collections that failed
    Phase 4a with CONFLICTING_DIM_SIZES but succeeded at Phase 4b."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "CRESCUE", num_granules=2)
    insert_collection(con, "CCLEAN")
    con.close()

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
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()
    assert (
        "**Rescued by Phase 4b:** 1 collection(s) that failed Phase 4a "
        "(`CONFLICTING_DIM_SIZES`) succeeded under Phase 4b." in text
    )


def test_render_report_includes_metadata_block(tmp_db_path, tmp_results_dir, tmp_path):
    """Report header lists survey tool version, dep versions, and sampling mode."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1")
    con.execute(
        "INSERT INTO run_meta (key, value, updated_at) VALUES ('sampling_mode', 'top=200', now())"
    )
    con.close()

    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [_row("C1", "G1")],
    )

    out = tmp_path / "report.md"
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()

    # Metadata block sits between the H1 and the Overview section.
    header, _, rest = text.partition("## Overview")
    assert "**Generated:**" in header
    assert "**Survey tool:**" in header
    assert "**VirtualiZarr:**" in header
    assert "**Sampling mode:** top=200" in header
    # Nothing from the first body section leaked into the header.
    assert "## Phase" not in header
    assert rest  # guard: Overview section actually present


def test_render_report_contains_counts(tmp_db_path, tmp_results_dir, tmp_path):
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1")
    insert_collection(con, "C2", num_granules=2)
    con.close()

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
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()
    assert "Phase 3: Parsability" in text
    assert "Phase 4a: Datasetability" in text
    assert "Phase 4b: Datatreeability" in text
    assert "Phase 5: Cubability" in text
    assert "all_pass" in text
    assert "PODAAC" in text
    assert "NetCDF4" in text
    assert "FEASIBLE" in text
    # Collections table enumerates concept IDs and includes datatree column
    assert "## Collections" in text
    assert (
        "| concept_id | daac | format | parse | dataset | datatree | cube | top_bucket |"
        in text
    )
    assert "| C1 |" in text
    assert "| C2 |" in text
    # Datatree taxonomy section present
    assert "Datatree Failure Taxonomy" in text
    # Interactive figures are embedded via iframes; figures/ directory exists
    assert 'src="figures/funnel.html"' in text
    assert (tmp_path / "figures").is_dir()
    assert (tmp_path / "figures" / "funnel.html").exists()
    assert (tmp_path / "figures" / "funnel.png").exists()


def test_render_report_incompatible_detection(tmp_db_path, tmp_results_dir, tmp_path):
    """Incompatible fingerprints (different dtypes) produce INCOMPATIBLE in the report."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "CINC", num_granules=2)
    con.close()

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
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()
    assert "INCOMPATIBLE" in text
    assert "dtype" in text


def test_taxonomy_counts_reports_granule_and_collection_counts(
    tmp_db_path, tmp_results_dir, tmp_path
):
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1", format_family="HDF4", format_declared="HDF")
    insert_collection(con, "C2", format_family="HDF4", format_declared="HDF")
    con.close()

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
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()
    # Expect "NO_PARSER | 6 | 2" (6 granules across 2 collections)
    assert "| NO_PARSER | 6 | 2 |" in text
    assert "| Bucket | Granules | Collections |" in text


def test_three_phase_daac_table_format(tmp_db_path, tmp_results_dir, tmp_path):
    """The By DAAC table should show Parsable/Datasetable/Cubable columns."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1")
    con.close()

    now = datetime.now(timezone.utc)
    _write_results(
        tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet",
        [
            _row("C1", "G1", now=now),
        ],
    )

    out = tmp_path / "report.md"
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()
    assert "By DAAC" in text
    assert "Parsable" in text
    assert "Datasetable" in text
    assert "Datatreeable" in text
    assert "Cubable" in text
    # PODAAC row should appear
    assert "PODAAC" in text


def test_three_phase_table_handles_missing_concept_id_in_cube_results():
    """A dataset-passing row whose concept_id is absent from cube_results
    must not crash _render_three_phase_table; it should be counted as
    cube-eligible-but-not-feasible (NOT_ATTEMPTED)."""
    from nasa_virtual_zarr_survey.report._markdown import _render_three_phase_table
    from nasa_virtual_zarr_survey.types import VerdictRow

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
    # 1 eligible row, 0 feasible -> "0/1 (0%)" in the Cubable column
    assert "| PODAAC | 1/1 (100%) | 1/1 (100%) | 1/1 (100%) | 0/1 (0%) |" in text


def test_l2_collection_gets_excluded_by_policy_cubability(tmp_db_path, tmp_results_dir):
    """An L2 collection that passes dataset all_pass is marked EXCLUDED_BY_POLICY,
    not run through the cubability check, since L2 swath products are not
    expected to combine into a single cube."""
    from nasa_virtual_zarr_survey.cubability import CubabilityVerdict
    from nasa_virtual_zarr_survey.report._aggregate import (
        cubability_results as _cubability_results,
    )

    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C_L2", num_granules=2, processing_level="L2")
    insert_collection(con, "C_L3", num_granules=2)

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

    verdicts = collection_verdicts(tmp_db_path, tmp_results_dir)
    con = connect(tmp_db_path)
    cube_results = _cubability_results(con, verdicts)
    con.close()

    assert cube_results["C_L2"].verdict == CubabilityVerdict.EXCLUDED_BY_POLICY
    assert "L2" in cube_results["C_L2"].reason
    # L3 still goes through the normal cubability machinery
    assert cube_results["C_L3"].verdict != CubabilityVerdict.EXCLUDED_BY_POLICY


def test_export_then_from_data_produces_identical_report(
    tmp_db_path, tmp_results_dir, tmp_path
):
    """Export a JSON digest, then regenerate from it; the two reports must be identical."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1")
    insert_collection(con, "C2", num_granules=2)
    con.close()

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
    # Step 1: run from DB, exporting digest alongside
    run_report(tmp_db_path, tmp_results_dir, out_path=out1, export_to=digest)
    assert digest.exists(), "export_to did not create the digest file"

    out2 = tmp_path / "index2.md"
    # Step 2: regenerate from digest only (db_path / results_dir are ignored)
    run_report(
        tmp_db_path,
        tmp_results_dir,
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
        "schema_version": 3,
        "created_at": "2026-04-26T12:00:00Z",
        "sampling_mode": "top=1",
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


def test_report_locked_sample_constructs_session(tmp_path: Path) -> None:
    sample_path = tmp_path / "locked.json"
    sample_path.write_text(json.dumps(_locked_sample_payload()))
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    out_path = tmp_path / "report.md"
    export_path = tmp_path / "summary.json"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "--locked-sample",
            str(sample_path),
            "--results",
            str(results_dir),
            "--out",
            str(out_path),
            "--export",
            str(export_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert export_path.exists()


def test_report_records_provenance_hashes(tmp_path: Path) -> None:
    sample_path = tmp_path / "locked.json"
    sample_path.write_text(json.dumps(_locked_sample_payload()))
    lock_path = tmp_path / "snapshot.uv.lock"
    lock_path.write_text("# fake uv.lock content\n")
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    export_path = tmp_path / "summary.json"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "--locked-sample",
            str(sample_path),
            "--results",
            str(results_dir),
            "--uv-lock",
            str(lock_path),
            "--snapshot-date",
            "2026-02-15",
            "--export",
            str(export_path),
            "--no-render",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(export_path.read_text())
    assert payload["schema_version"] == 7
    assert payload["snapshot_date"] == "2026-02-15"
    assert payload["snapshot_kind"] == "release"
    assert payload["locked_sample_sha256"] == _sha256_of(sample_path)
    assert payload["uv_lock_sha256"] == _sha256_of(lock_path)


def test_report_preview_manifest_records_metadata(tmp_path: Path) -> None:
    sample_path = tmp_path / "locked.json"
    sample_path.write_text(json.dumps(_locked_sample_payload()))

    manifest = tmp_path / "preview.toml"
    manifest.write_text(
        'snapshot_date = "2026-04-26"\n'
        'label = "variable-chunking"\n'
        'description = "Coordinated branches"\n'
        "[git_overrides]\n"
        'virtualizarr = { url = "https://github.com/zarr-developers/VirtualiZarr", rev = "abc123de" }\n'
    )

    results_dir = tmp_path / "results"
    results_dir.mkdir()
    export_path = tmp_path / "summary.json"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "--locked-sample",
            str(sample_path),
            "--results",
            str(results_dir),
            "--preview-manifest",
            str(manifest),
            "--export",
            str(export_path),
            "--no-render",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(export_path.read_text())
    assert payload["snapshot_kind"] == "preview"
    assert payload["snapshot_date"] == "2026-04-26"
    assert payload["label"] == "variable-chunking"
    assert payload["description"] == "Coordinated branches"
    assert payload["git_overrides"] == {
        "virtualizarr": {
            "url": "https://github.com/zarr-developers/VirtualiZarr",
            "rev": "abc123de",
        }
    }
    assert payload["uv_lock_sha256"] is None


def test_report_no_render_skips_markdown(tmp_path: Path) -> None:
    sample_path = tmp_path / "locked.json"
    sample_path.write_text(json.dumps(_locked_sample_payload()))
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    out_path = tmp_path / "report.md"
    export_path = tmp_path / "summary.json"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "--locked-sample",
            str(sample_path),
            "--results",
            str(results_dir),
            "--out",
            str(out_path),
            "--export",
            str(export_path),
            "--no-render",
        ],
    )
    assert result.exit_code == 0, result.output
    assert export_path.exists()
    assert not out_path.exists()


def test_report_uv_lock_and_preview_manifest_mutually_exclusive(tmp_path: Path) -> None:
    sample_path = tmp_path / "locked.json"
    sample_path.write_text(json.dumps(_locked_sample_payload()))
    manifest = tmp_path / "preview.toml"
    manifest.write_text(
        'snapshot_date = "2026-04-26"\nlabel = "x"\n'
        '[git_overrides]\nvz = {url="u", rev="abc1234"}\n'
    )
    lock_path = tmp_path / "uv.lock"
    lock_path.write_text("# stub\n")
    results_dir = tmp_path / "results"
    results_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "--locked-sample",
            str(sample_path),
            "--results",
            str(results_dir),
            "--uv-lock",
            str(lock_path),
            "--preview-manifest",
            str(manifest),
            "--export",
            str(tmp_path / "summary.json"),
            "--no-render",
        ],
    )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def _touch_cache_entry(cache_dir: Path, url: str) -> Path:
    """Create the on-disk file ``DiskCachingReadableStore`` would write for *url*."""
    from nasa_virtual_zarr_survey.cache import cache_layout_path

    p = cache_layout_path(cache_dir, url)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"")
    return p


def _seed_cache_only_fixtures(tmp_db_path: Path, tmp_results_dir: Path) -> None:
    """Two collections, two granules each, all parse+dataset success."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C_HIT", num_granules=2)
    insert_collection(con, "C_MISS", num_granules=2)
    insert_granule(con, "C_HIT", "G_HIT_A", data_url="s3://b/hit-a.nc")
    insert_granule(con, "C_HIT", "G_HIT_B", data_url="s3://b/hit-b.nc")
    insert_granule(con, "C_MISS", "G_MISS_A", data_url="s3://b/miss-a.nc")
    insert_granule(con, "C_MISS", "G_MISS_B", data_url="s3://b/miss-b.nc")
    con.close()

    now = datetime.now(timezone.utc)
    rows = [
        _row("C_HIT", "G_HIT_A", parse_success=True, dataset_success=True, now=now),
        _row("C_HIT", "G_HIT_B", parse_success=True, dataset_success=True, now=now),
        _row("C_MISS", "G_MISS_A", parse_success=True, dataset_success=True, now=now),
        _row("C_MISS", "G_MISS_B", parse_success=True, dataset_success=True, now=now),
    ]
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", rows)


def test_run_report_cache_only_filters_to_cached_granules(
    tmp_db_path, tmp_results_dir, tmp_path
):
    """When only one collection's granules are cached, only it appears in verdicts."""
    _seed_cache_only_fixtures(tmp_db_path, tmp_results_dir)
    cache_dir = tmp_path / "cache"
    _touch_cache_entry(cache_dir, "s3://b/hit-a.nc")
    _touch_cache_entry(cache_dir, "s3://b/hit-b.nc")

    out_path = tmp_path / "report.md"
    run_report(
        tmp_db_path,
        results_dir=tmp_results_dir,
        out_path=out_path,
        cache_dir=cache_dir,
        cache_only=True,
    )
    verdicts = {
        v["concept_id"]: v for v in collection_verdicts(tmp_db_path, tmp_results_dir)
    }
    # Reading without the cache filter table on a fresh con shows full results.
    assert verdicts["C_HIT"]["dataset_verdict"] == "all_pass"
    assert verdicts["C_MISS"]["dataset_verdict"] == "all_pass"

    # The rendered report reflects the filtered view: C_MISS is dropped
    # entirely (no cached granules), and overview totals reflect that.
    text = out_path.read_text()
    assert not any(line.startswith("| C_MISS |") for line in text.splitlines())
    hit_line = next(line for line in text.splitlines() if line.startswith("| C_HIT |"))
    assert "all_pass" in hit_line
    assert "Total collections: **1**" in text


def test_run_report_cache_only_with_no_cached_granules(
    tmp_db_path, tmp_results_dir, tmp_path
):
    """No cached files -> overview totals report zero collections; table is empty."""
    _seed_cache_only_fixtures(tmp_db_path, tmp_results_dir)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    out_path = tmp_path / "report.md"
    run_report(
        tmp_db_path,
        results_dir=tmp_results_dir,
        out_path=out_path,
        cache_dir=cache_dir,
        cache_only=True,
    )
    text = out_path.read_text()
    assert "Total collections: **0**" in text
    for cid in ("C_HIT", "C_MISS"):
        assert not any(line.startswith(f"| {cid} |") for line in text.splitlines())


def test_run_report_cache_only_all_cached_matches_unfiltered(
    tmp_db_path, tmp_results_dir, tmp_path
):
    """When every granule is cached, --cache-only output equals the unfiltered run."""
    _seed_cache_only_fixtures(tmp_db_path, tmp_results_dir)
    cache_dir = tmp_path / "cache"
    for url in (
        "s3://b/hit-a.nc",
        "s3://b/hit-b.nc",
        "s3://b/miss-a.nc",
        "s3://b/miss-b.nc",
    ):
        _touch_cache_entry(cache_dir, url)

    filtered = tmp_path / "filtered.md"
    plain = tmp_path / "plain.md"
    run_report(
        tmp_db_path,
        results_dir=tmp_results_dir,
        out_path=filtered,
        cache_dir=cache_dir,
        cache_only=True,
    )
    run_report(tmp_db_path, results_dir=tmp_results_dir, out_path=plain)

    # The 'Generated' line carries a timestamp; strip it before comparing.
    def _strip_generated(s: str) -> str:
        return "\n".join(
            line for line in s.splitlines() if not line.startswith("- **Generated:")
        )

    assert _strip_generated(filtered.read_text()) == _strip_generated(plain.read_text())


def test_run_report_cache_only_requires_cache_dir(
    tmp_db_path, tmp_results_dir, tmp_path
):
    import pytest

    with pytest.raises(ValueError, match="cache_only=True requires cache_dir"):
        run_report(
            tmp_db_path,
            results_dir=tmp_results_dir,
            out_path=tmp_path / "report.md",
            cache_only=True,
        )


def test_run_report_cache_only_rejects_from_data(tmp_path):
    import pytest

    digest = tmp_path / "summary.json"
    digest.write_text("{}")  # contents irrelevant; validation runs first
    with pytest.raises(ValueError, match="cache_only and from_data are mutually"):
        run_report(
            None,
            results_dir=tmp_path / "results",
            out_path=tmp_path / "report.md",
            from_data=digest,
            cache_dir=tmp_path / "cache",
            cache_only=True,
        )


def test_report_cli_cache_only_mutex_with_from_data(tmp_path):
    digest = tmp_path / "summary.json"
    digest.write_text("{}")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "--from-data",
            str(digest),
            "--cache-only",
            "--out",
            str(tmp_path / "report.md"),
        ],
    )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def test_report_cli_cache_only_requires_cache(tmp_path):
    sample_path = tmp_path / "locked.json"
    sample_path.write_text(json.dumps(_locked_sample_payload()))
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "--locked-sample",
            str(sample_path),
            "--results",
            str(results_dir),
            "--no-cache",
            "--cache-only",
            "--out",
            str(tmp_path / "report.md"),
        ],
    )
    assert result.exit_code != 0
    assert "--cache-only requires --cache" in result.output
