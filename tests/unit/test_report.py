from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.report import collection_verdicts, run_report


def _write_results(path: Path, rows: list[dict]) -> None:
    schema = pa.schema([
        ("collection_concept_id", pa.string()),
        ("granule_concept_id", pa.string()),
        ("daac", pa.string()),
        ("format_family", pa.string()),
        ("parser", pa.string()),
        ("success", pa.bool_()),
        ("error_type", pa.string()),
        ("error_message", pa.string()),
        ("error_traceback", pa.string()),
        ("duration_s", pa.float64()),
        ("timed_out", pa.bool_()),
        ("attempted_at", pa.timestamp("us", tz="UTC")),
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    pq.write_table(pa.table(cols, schema=schema), path)


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
    for i in range(3):
        rows.append({"collection_concept_id": "C_ALL", "granule_concept_id": f"G{i}",
                     "daac": "PODAAC", "format_family": "NetCDF4", "parser": "HDFParser",
                     "success": True, "error_type": None, "error_message": None,
                     "error_traceback": None, "duration_s": 0.1, "timed_out": False,
                     "attempted_at": now})
    for i in range(3):
        rows.append({"collection_concept_id": "C_PART", "granule_concept_id": f"G{i}",
                     "daac": "PODAAC", "format_family": "NetCDF4", "parser": "HDFParser",
                     "success": i == 0, "error_type": None if i == 0 else "ValueError",
                     "error_message": None if i == 0 else "codec foo not supported",
                     "error_traceback": None, "duration_s": 0.1, "timed_out": False,
                     "attempted_at": now})
    for i in range(3):
        rows.append({"collection_concept_id": "C_NONE", "granule_concept_id": f"G{i}",
                     "daac": "PODAAC", "format_family": "NetCDF4", "parser": "HDFParser",
                     "success": False, "error_type": "PermissionError",
                     "error_message": "403 Forbidden", "error_traceback": None,
                     "duration_s": 0.1, "timed_out": False, "attempted_at": now})
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", rows)

    verdicts = collection_verdicts(tmp_db_path, tmp_results_dir)
    by_id = {v["concept_id"]: v for v in verdicts}
    assert by_id["C_ALL"]["verdict"] == "all_pass"
    assert by_id["C_PART"]["verdict"] == "partial_pass"
    assert by_id["C_NONE"]["verdict"] == "all_fail"
    assert by_id["C_SKIP"]["verdict"] == "skipped_format"
    # stratified is None because no granule rows were inserted into the DB
    assert by_id["C_ALL"]["stratified"] is None


def test_render_report_contains_counts(tmp_db_path, tmp_results_dir, tmp_path):
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',1,NULL,NULL,'L3',NULL,now())"
    )
    con.close()

    now = datetime.now(timezone.utc)
    _write_results(tmp_results_dir / "DAAC=PODAAC" / "part-0000.parquet", [
        {"collection_concept_id": "C1", "granule_concept_id": "G1", "daac": "PODAAC",
         "format_family": "NetCDF4", "parser": "HDFParser", "success": True,
         "error_type": None, "error_message": None, "error_traceback": None,
         "duration_s": 0.1, "timed_out": False, "attempted_at": now}
    ])

    out = tmp_path / "report.md"
    run_report(tmp_db_path, tmp_results_dir, out)
    text = out.read_text()
    assert "all_pass" in text
    assert "PODAAC" in text
    assert "NetCDF4" in text
    assert "Stratification" in text
