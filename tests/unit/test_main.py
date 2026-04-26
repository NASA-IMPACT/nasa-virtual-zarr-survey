"""Unit tests for nasa_virtual_zarr_survey.__main__ Click commands."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner


def _setup_db_with_skipped_collection(db_path: Path) -> None:
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C-SKIPPED', 'n', '1', 'ASF', 'ASF', NULL, 'mystery', 1, "
        "NULL, NULL, 'L1', 'format_unknown', now())"
    )
    con.close()


def _setup_db_with_unattempted_collection(db_path: Path) -> None:
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(db_path)
    init_schema(con)
    # Array-like collection but no granules sampled.
    con.execute(
        "INSERT INTO collections VALUES "
        "('C-LIVE', 'n', '1', 'PODAAC', 'PODAAC', 'NetCDF4', 'NetCDF-4', 1, "
        "NULL, NULL, 'L3', NULL, now())"
    )
    con.close()


def _write_attempt_shard_for_collection(
    results_dir: Path, *, collection_concept_id: str, success: bool
) -> None:
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    shard_dir = results_dir / "DAAC=PODAAC"
    shard_dir.mkdir(parents=True, exist_ok=True)
    cols = {f.name: [] for f in _SCHEMA}
    cols["collection_concept_id"].append(collection_concept_id)
    cols["granule_concept_id"].append("G-OK")
    cols["daac"].append("PODAAC")
    cols["format_family"].append("NetCDF4")
    cols["parser"].append("HDFParser")
    cols["stratified"].append(True)
    cols["attempted_at"].append(datetime.now(timezone.utc))
    cols["parse_success"].append(success)
    cols["parse_error_type"].append(None if success else "OSError")
    cols["parse_error_message"].append(None if success else "boom")
    cols["parse_error_traceback"].append(None)
    cols["parse_duration_s"].append(0.1)
    cols["dataset_success"].append(success)
    cols["dataset_error_type"].append(None)
    cols["dataset_error_message"].append(None)
    cols["dataset_error_traceback"].append(None)
    cols["dataset_duration_s"].append(0.0)
    cols["datatree_success"].append(None)
    cols["datatree_error_type"].append(None)
    cols["datatree_error_message"].append(None)
    cols["datatree_error_traceback"].append(None)
    cols["datatree_duration_s"].append(0.0)
    cols["success"].append(success)
    cols["override_applied"].append(False)
    cols["timed_out"].append(False)
    cols["timed_out_phase"].append(None)
    cols["duration_s"].append(0.1)
    cols["fingerprint"].append(None)
    pq.write_table(pa.table(cols, schema=_SCHEMA), shard_dir / "part-0000.parquet")


def test_repro_no_failures_skipped_collection_shows_probe_hint(
    tmp_path: Path,
) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    db_path = tmp_path / "survey.duckdb"
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    _setup_db_with_skipped_collection(db_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "repro",
            "C-SKIPPED",
            "--db",
            str(db_path),
            "--results",
            str(results_dir),
        ],
    )
    assert result.exit_code != 0
    assert "No matching failures found." in result.output
    assert "skip_reason='format_unknown'" in result.output
    assert "nasa-virtual-zarr-survey probe C-SKIPPED" in result.output


def test_repro_no_failures_unattempted_collection_shows_probe_hint(
    tmp_path: Path,
) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    db_path = tmp_path / "survey.duckdb"
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    _setup_db_with_unattempted_collection(db_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "repro",
            "C-LIVE",
            "--db",
            str(db_path),
            "--results",
            str(results_dir),
        ],
    )
    assert result.exit_code != 0
    assert "No matching failures found." in result.output
    assert "nasa-virtual-zarr-survey probe C-LIVE" in result.output


def test_repro_no_failures_all_succeeded_no_probe_hint(tmp_path: Path) -> None:
    """Concept ID has Parquet rows that simply don't match the failure filter →
    original 'No matching failures found' message, no hint."""
    from nasa_virtual_zarr_survey.__main__ import cli
    from nasa_virtual_zarr_survey.db import connect, init_schema

    db_path = tmp_path / "survey.duckdb"
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    con = connect(db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C-OK', 'n', '1', 'PODAAC', 'PODAAC', 'NetCDF4', 'NetCDF-4', 1, "
        "NULL, NULL, 'L3', NULL, now())"
    )
    now = datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO granules VALUES ('C-OK', 'G-OK', 'https://x/y.nc', NULL, "
        "0, NULL, ?, true, 'external')",
        [now],
    )
    con.close()
    _write_attempt_shard_for_collection(
        results_dir, collection_concept_id="C-OK", success=True
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "repro",
            "C-OK",
            "--db",
            str(db_path),
            "--results",
            str(results_dir),
        ],
    )
    assert result.exit_code != 0
    assert "No matching failures found." in result.output
    # Granule + parquet row exist → no hint.
    assert "probe" not in result.output


def test_probe_command_writes_script_to_out_dir(tmp_path: Path) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli
    from nasa_virtual_zarr_survey.db import connect, init_schema

    db_path = tmp_path / "survey.duckdb"
    out_dir = tmp_path / "probes"

    con = connect(db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C-FULL', 'n', '1', 'PODAAC', 'PODAAC', 'NetCDF4', 'NetCDF-4', 1, "
        "NULL, NULL, 'L3', NULL, now())"
    )
    now = datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO granules VALUES ('C-FULL', 'G-FULL-0', "
        "'s3://b/file0.h5', NULL, 0, NULL, ?, true, 'direct')",
        [now],
    )
    con.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["probe", "C-FULL", "--db", str(db_path), "--out", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    [script] = list(out_dir.glob("*.py"))
    assert script.name == "probe_C-FULL.py"
    text = script.read_text()
    assert "# --- imports ---" in text
    assert "C-FULL" in text


def test_probe_command_to_stdout(tmp_path: Path) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli
    from nasa_virtual_zarr_survey.db import connect, init_schema

    db_path = tmp_path / "survey.duckdb"
    con = connect(db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C-FULL', 'n', '1', 'PODAAC', 'PODAAC', 'NetCDF4', 'NetCDF-4', 1, "
        "NULL, NULL, 'L3', NULL, now())"
    )
    now = datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO granules VALUES ('C-FULL', 'G-FULL-0', "
        "'s3://b/file0.h5', NULL, 0, NULL, ?, true, 'direct')",
        [now],
    )
    con.close()

    runner = CliRunner()
    result = runner.invoke(cli, ["probe", "C-FULL", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "# --- imports ---" in result.output
    assert "C-FULL" in result.output
