"""Unit tests for nasa_virtual_zarr_survey.__main__ Click commands."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from nasa_virtual_zarr_survey import opendap
from nasa_virtual_zarr_survey.db import connect, init_schema
from tests.conftest import insert_collection, insert_granule


@pytest.fixture(autouse=True)
def _stub_opendap_service_ids(monkeypatch):
    """Default to an empty cloud-OPeNDAP set; individual tests can override."""
    opendap.cloud_opendap_service_ids.cache_clear()
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.discover.cloud_opendap_service_ids",
        lambda: frozenset(),
    )
    yield
    opendap.cloud_opendap_service_ids.cache_clear()


def _umm(
    cid: str,
    *,
    fmt: str | None = "NetCDF-4",
    plevel: str | None = "L3",
    daac: str = "PODAAC",
    sn: str = "FOO",
    ver: str = "1",
) -> dict[str, Any]:
    archive: dict[str, Any] = {}
    if fmt is not None:
        archive["FileDistributionInformation"] = [{"Format": fmt}]
    inner: dict[str, Any] = {
        "ShortName": sn,
        "Version": ver,
        "DataCenters": [{"ShortName": daac}],
        "ArchiveAndDistributionInformation": archive,
    }
    if plevel is not None:
        inner["ProcessingLevel"] = {"Id": plevel}
    return {"meta": {"concept-id": cid, "provider-id": daac}, "umm": inner}


class _FakeColl:
    def __init__(self, d: dict[str, Any]) -> None:
        self.render_dict = d


def _setup_db_with_skipped_collection(db_path: Path) -> None:
    con = connect(db_path)
    init_schema(con)
    insert_collection(
        con,
        "C-SKIPPED",
        short_name="n",
        daac="ASF",
        format_family=None,
        format_declared="mystery",
        processing_level="L1",
        skip_reason="format_unknown",
    )
    con.close()


def _setup_db_with_unattempted_collection(db_path: Path) -> None:
    con = connect(db_path)
    init_schema(con)
    # Array-like collection but no granules sampled.
    insert_collection(con, "C-LIVE", short_name="n")
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

    db_path = tmp_path / "survey.duckdb"
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    con = connect(db_path)
    init_schema(con)
    insert_collection(con, "C-OK", short_name="n")
    insert_granule(
        con,
        "C-OK",
        "G-OK",
        data_url="https://x/y.nc",
        sampled_at=datetime.now(timezone.utc),
        access_mode="external",
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

    db_path = tmp_path / "survey.duckdb"
    out_dir = tmp_path / "probes"

    con = connect(db_path)
    init_schema(con)
    insert_collection(con, "C-FULL", short_name="n")
    insert_granule(
        con,
        "C-FULL",
        "G-FULL-0",
        data_url="s3://b/file0.h5",
        sampled_at=datetime.now(timezone.utc),
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

    db_path = tmp_path / "survey.duckdb"
    con = connect(db_path)
    init_schema(con)
    insert_collection(con, "C-FULL", short_name="n")
    insert_granule(
        con,
        "C-FULL",
        "G-FULL-0",
        data_url="s3://b/file0.h5",
        sampled_at=datetime.now(timezone.utc),
    )
    con.close()

    runner = CliRunner()
    result = runner.invoke(cli, ["probe", "C-FULL", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "# --- imports ---" in result.output
    assert "C-FULL" in result.output


# ---------------------------------------------------------------------------
# discover --list
# ---------------------------------------------------------------------------


def _patch_search(monkeypatch, dicts: list[dict[str, Any]]) -> None:
    fake = MagicMock(return_value=[_FakeColl(d) for d in dicts])
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.discover.earthaccess.search_datasets", fake
    )


def test_discover_list_none_emits_only_aggregate(tmp_path: Path, monkeypatch) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    _patch_search(monkeypatch, [_umm("C1-PODAAC"), _umm("C2-PODAAC", fmt="PDF")])

    runner = CliRunner()
    result = runner.invoke(
        cli, ["discover", "--limit", "2", "--list", "none", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "discover (dry-run): 2 collections" in result.output
    assert "rank" not in result.output
    assert "concept_id" not in result.output
    assert "C1-PODAAC" not in result.output


def test_discover_list_array_excludes_skipped(tmp_path: Path, monkeypatch) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    _patch_search(
        monkeypatch,
        [
            _umm("C-OK-PODAAC"),
            _umm("C-SKIP-PODAAC", fmt="PDF"),
        ],
    )

    runner = CliRunner()
    result = runner.invoke(
        cli, ["discover", "--limit", "2", "--list", "array", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "C-OK-PODAAC" in result.output
    assert "C-SKIP-PODAAC" not in result.output
    # In non-top mode rank/usage_score columns exist as headers but values are blank.
    assert "rank" in result.output
    assert "usage_score" in result.output
    # No skip_reason column populated for array-like rows.
    assert "non_array_format" not in result.output


def test_discover_list_all_includes_skip_reason_column(
    tmp_path: Path, monkeypatch
) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    _patch_search(
        monkeypatch,
        [
            _umm("C-OK-PODAAC"),
            _umm("C-SKIP-PODAAC", fmt="PDF"),
        ],
    )

    runner = CliRunner()
    result = runner.invoke(
        cli, ["discover", "--limit", "2", "--list", "all", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "C-OK-PODAAC" in result.output
    assert "C-SKIP-PODAAC" in result.output
    assert "non_array_format" in result.output
    assert "https://search.earthdata.nasa.gov/search?q=C-OK-PODAAC" in result.output


def test_discover_list_includes_opendap_column(tmp_path: Path, monkeypatch) -> None:
    """The --list table renders 'Y' for collections with cloud OPeNDAP."""
    from nasa_virtual_zarr_survey.__main__ import cli

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.discover.cloud_opendap_service_ids",
        lambda: frozenset({"S-OPENDAP"}),
    )
    od_umm = _umm("C-OD-PODAAC", sn="WITHDMRPP")
    od_umm["meta"]["associations"] = {"services": ["S-OPENDAP"]}
    no_umm = _umm("C-NO-PODAAC", sn="NODMRPP")

    _patch_search(monkeypatch, [od_umm, no_umm])

    runner = CliRunner()
    result = runner.invoke(
        cli, ["discover", "--limit", "2", "--list", "array", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "opendap" in result.output  # header
    out = result.output
    # The opendap-having row gets 'Y'; the non-opendap row's column is empty.
    od_line = next(line for line in out.splitlines() if "C-OD-PODAAC" in line)
    no_line = next(line for line in out.splitlines() if "C-NO-PODAAC" in line)
    # 'Y' marker present on the opendap line, absent on the other.
    assert " Y " in f" {od_line} "
    assert " Y " not in f" {no_line} "


def test_discover_list_skipped_shows_breakdown_and_table(
    tmp_path: Path, monkeypatch
) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    _patch_search(
        monkeypatch,
        [
            _umm("C-OK-PODAAC"),
            _umm("C-PDF-PODAAC", fmt="PDF"),
            _umm("C-NULL-PODAAC", fmt=None),
        ],
    )

    runner = CliRunner()
    result = runner.invoke(
        cli, ["discover", "--limit", "3", "--list", "skipped", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "Skipped collections by format:" in result.output
    # Table excludes the array-like row
    assert "C-OK-PODAAC" not in result.output
    assert "C-PDF-PODAAC" in result.output
    assert "C-NULL-PODAAC" in result.output
    assert "non_array_format" in result.output
    assert "format_unknown" in result.output


def test_discover_list_top_mode_sorts_by_rank(tmp_path: Path, monkeypatch) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.popularity.all_top_collection_ids",
        lambda providers, num_per_provider=100: [
            ("C-HIGH-POCLOUD", 18000),
            ("C-MID-POCLOUD", 9000),
            ("C-LOW-POCLOUD", 100),
        ],
    )
    _patch_search(
        monkeypatch,
        [
            _umm("C-HIGH-POCLOUD", sn="HIGH"),
            _umm("C-MID-POCLOUD", sn="MID"),
            _umm("C-LOW-POCLOUD", sn="LOW"),
        ],
    )

    runner = CliRunner()
    result = runner.invoke(
        cli, ["discover", "--top-per-provider", "3", "--list", "array", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    # Each rank/score appears.
    assert "18000" in result.output
    assert "9000" in result.output
    assert "100" in result.output
    # Order in output: HIGH before MID before LOW.
    out = result.output
    assert out.index("C-HIGH-POCLOUD") < out.index("C-MID-POCLOUD")
    assert out.index("C-MID-POCLOUD") < out.index("C-LOW-POCLOUD")


def test_discover_list_persists_db_when_not_dry_run(
    tmp_path: Path, monkeypatch
) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    db_path = tmp_path / "survey.duckdb"
    _patch_search(monkeypatch, [_umm("C1-PODAAC")])

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "discover",
            "--limit",
            "1",
            "--db",
            str(db_path),
            "--list",
            "array",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "C1-PODAAC" in result.output

    con = connect(db_path)
    init_schema(con)
    n = con.execute("SELECT count(*) FROM collections").fetchone()[0]
    assert n == 1
    sm = con.execute(
        "SELECT value FROM run_meta WHERE key = 'sampling_mode'"
    ).fetchone()
    assert sm == ("limit=1",)


def test_discover_skipped_flag_no_longer_accepted(tmp_path: Path, monkeypatch) -> None:
    from nasa_virtual_zarr_survey.__main__ import cli

    _patch_search(monkeypatch, [_umm("C1-PODAAC")])

    runner = CliRunner()
    result = runner.invoke(cli, ["discover", "--limit", "1", "--skipped", "--dry-run"])
    assert result.exit_code != 0
    assert "no such option" in result.output.lower() or "--skipped" in result.output
