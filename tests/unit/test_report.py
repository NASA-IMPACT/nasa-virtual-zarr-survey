from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nasa_virtual_zarr_survey.cubability import fingerprint_to_json
from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.report import collection_verdicts, run_report

# New schema matching attempt._SCHEMA
_RESULT_SCHEMA = pa.schema(
    [
        ("collection_concept_id", pa.string()),
        ("granule_concept_id", pa.string()),
        ("daac", pa.string()),
        ("format_family", pa.string()),
        ("parser", pa.string()),
        ("stratified", pa.bool_()),
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
    parse_error_type: str | None = None,
    parse_error_message: str | None = None,
    dataset_error_type: str | None = None,
    dataset_error_message: str | None = None,
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
        "stratified": True,
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
        "success": bool(parse_success and dataset_success),
        "timed_out": False,
        "timed_out_phase": None,
        "duration_s": 0.2,
        "fingerprint": fingerprint,
    }


def test_collection_verdicts_classifies_all_three(tmp_db_path, tmp_results_dir):
    con = connect(tmp_db_path)
    init_schema(con)
    for cid in ["C_ALL", "C_PART", "C_NONE"]:
        con.execute(
            f"INSERT INTO collections VALUES ('{cid}','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',5,NULL,NULL,'L3',NULL,now())"
        )
    con.execute(
        "INSERT INTO collections VALUES ('C_SKIP','s','1','PODAAC','PODAAC',NULL,'PDF',5,NULL,NULL,'L3','non_array_format',now())"
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
    # stratified is None because no granule rows were inserted into the DB
    assert by_id["C_ALL"]["stratified"] is None


def test_parse_fail_means_dataset_not_attempted(tmp_db_path, tmp_results_dir):
    """When parse fails for all granules, dataset should show not_attempted."""
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES ('C_FAIL','s','1','PODAAC','PODAAC','HDF4','HDF',2,NULL,NULL,'L3',NULL,now())"
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


def test_render_report_contains_counts(tmp_db_path, tmp_results_dir, tmp_path):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',1,NULL,NULL,'L3',NULL,now())"
    )
    con.execute(
        "INSERT INTO collections VALUES ('C2','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',2,NULL,NULL,'L3',NULL,now())"
    )
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
    assert "Phase 4: Datasetability" in text
    assert "Phase 5: Virtual Store Feasibility" in text
    assert "all_pass" in text
    assert "PODAAC" in text
    assert "NetCDF4" in text
    assert "Stratification" in text
    assert "FEASIBLE" in text
    # Collections table enumerates concept IDs
    assert "## Collections" in text
    assert (
        "| concept_id | daac | format | parse | dataset | cube | top_bucket |" in text
    )
    assert "| C1 |" in text
    assert "| C2 |" in text


def test_render_report_incompatible_detection(tmp_db_path, tmp_results_dir, tmp_path):
    """Incompatible fingerprints (different dtypes) produce INCOMPATIBLE in the report."""
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES ('CINC','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',2,NULL,NULL,'L3',NULL,now())"
    )
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
    con.execute(
        "INSERT INTO collections VALUES ('C1','s','1','PODAAC','PODAAC','HDF4','HDF',1,NULL,NULL,'L3',NULL,now())"
    )
    con.execute(
        "INSERT INTO collections VALUES ('C2','s','1','PODAAC','PODAAC','HDF4','HDF',1,NULL,NULL,'L3',NULL,now())"
    )
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
    con.execute(
        "INSERT INTO collections VALUES ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',1,NULL,NULL,'L3',NULL,now())"
    )
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
    assert "Cubable" in text
    # PODAAC row should appear
    assert "PODAAC" in text
