"""Unit tests for vzc.__main__ Click commands."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from vzc.cmr import _opendap as opendap
from vzc.state._io import load_state, save_state
from tests.conftest import make_collection, make_granule, make_state


@pytest.fixture(autouse=True)
def _stub_opendap_service_ids(monkeypatch):
    opendap.cloud_opendap_service_ids.cache_clear()
    monkeypatch.setattr(
        "vzc.cmr._discover.cloud_opendap_service_ids",
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


def _seed_state(state_path: Path, **kwargs: Any) -> None:
    state = make_state(**kwargs)
    save_state(state, state_path)


def _seed_skipped_collection(state_path: Path) -> None:
    _seed_state(
        state_path,
        collections=[
            make_collection(
                "C-SKIPPED",
                short_name="n",
                daac="ASF",
                format_family=None,
                format_declared="mystery",
                processing_level="L1",
                skip_reason="format_unknown",
            )
        ],
    )


def _seed_unattempted_collection(state_path: Path) -> None:
    _seed_state(
        state_path,
        collections=[make_collection("C-LIVE", short_name="n")],
    )


def _write_attempt_shard_for_collection(
    results_dir: Path, *, collection_concept_id: str, success: bool
) -> None:
    from vzc.pipeline._attempt import _SCHEMA

    shard_dir = results_dir / "DAAC=PODAAC"
    shard_dir.mkdir(parents=True, exist_ok=True)
    cols = {f.name: [] for f in _SCHEMA}
    cols["collection_concept_id"].append(collection_concept_id)
    cols["granule_concept_id"].append("G-OK")
    cols["daac"].append("PODAAC")
    cols["format_family"].append("NetCDF4")
    cols["parser"].append("HDFParser")
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


# ---------------------------------------------------------------------------
# investigate (replaces probe + repro)
# ---------------------------------------------------------------------------


def _seed_full_collection(state_path: Path) -> None:
    _seed_state(
        state_path,
        collections=[make_collection("C-FULL", short_name="n")],
        granules=[
            make_granule(
                "C-FULL",
                "G-FULL-0",
                s3_url="s3://b/file0.h5",
                https_url="https://b/file0.h5",
                sampled_at=datetime.now(timezone.utc),
            )
        ],
    )


def test_investigate_native_writes_script_to_out_path(
    tmp_path: Path, monkeypatch
) -> None:
    from vzc.__main__ import cli

    monkeypatch.chdir(tmp_path)
    state_path = tmp_path / "output" / "state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    out_path = tmp_path / "probes" / "investigate.py"
    _seed_full_collection(state_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["investigate", "C-FULL", "--mode", "native", "--out", str(out_path)],
    )
    assert result.exit_code == 0, result.output
    text = out_path.read_text()
    assert "# --- imports ---" in text
    assert "C-FULL" in text


def test_investigate_native_to_stdout(tmp_path: Path, monkeypatch) -> None:
    from vzc.__main__ import cli

    monkeypatch.chdir(tmp_path)
    state_path = tmp_path / "output" / "state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    _seed_full_collection(state_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["investigate", "C-FULL", "--mode", "native"])
    assert result.exit_code == 0, result.output
    assert "# --- imports ---" in result.output
    assert "C-FULL" in result.output


def test_investigate_virtual_to_stdout(tmp_path: Path, monkeypatch) -> None:
    """Default mode (virtual) emits a script that imports attempt_one."""
    from vzc.__main__ import cli

    monkeypatch.chdir(tmp_path)
    state_path = tmp_path / "output" / "state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    _seed_full_collection(state_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["investigate", "C-FULL", "--access", "direct"])
    assert result.exit_code == 0, result.output
    assert "from vzc.pipeline._attempt import attempt_one" in result.output
    assert "C-FULL" in result.output


# ---------------------------------------------------------------------------
# discover --list
# ---------------------------------------------------------------------------


def _patch_search(monkeypatch, dicts: list[dict[str, Any]]) -> None:
    fake = MagicMock(return_value=[_FakeColl(d) for d in dicts])
    monkeypatch.setattr("vzc.cmr._discover.earthaccess.search_datasets", fake)


def test_discover_list_none_emits_only_aggregate(tmp_path: Path, monkeypatch) -> None:
    from vzc.__main__ import cli

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
    from vzc.__main__ import cli

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
    assert "rank" in result.output
    assert "usage_score" in result.output
    assert "non_array_format" not in result.output


def test_discover_list_all_includes_skip_reason_column(
    tmp_path: Path, monkeypatch
) -> None:
    from vzc.__main__ import cli

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
    from vzc.__main__ import cli

    monkeypatch.setattr(
        "vzc.cmr._discover.cloud_opendap_service_ids",
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
    assert "opendap" in result.output
    out = result.output
    od_line = next(line for line in out.splitlines() if "C-OD-PODAAC" in line)
    no_line = next(line for line in out.splitlines() if "C-NO-PODAAC" in line)
    assert " Y " in f" {od_line} "
    assert " Y " not in f" {no_line} "


def test_discover_list_skipped_shows_breakdown_and_table(
    tmp_path: Path, monkeypatch
) -> None:
    from vzc.__main__ import cli

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
    assert "C-OK-PODAAC" not in result.output
    assert "C-PDF-PODAAC" in result.output
    assert "C-NULL-PODAAC" in result.output
    assert "non_array_format" in result.output
    assert "format_unknown" in result.output


def test_discover_list_top_mode_sorts_by_rank(tmp_path: Path, monkeypatch) -> None:
    from vzc.__main__ import cli

    monkeypatch.setattr(
        "vzc.cmr._popularity.all_top_collection_ids",
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
    assert "18000" in result.output
    assert "9000" in result.output
    assert "100" in result.output
    out = result.output
    assert out.index("C-HIGH-POCLOUD") < out.index("C-MID-POCLOUD")
    assert out.index("C-MID-POCLOUD") < out.index("C-LOW-POCLOUD")


def test_discover_list_persists_state_when_not_dry_run(
    tmp_path: Path, monkeypatch
) -> None:
    from vzc.__main__ import cli

    monkeypatch.chdir(tmp_path)
    state_path = tmp_path / "output" / "state.json"
    _patch_search(monkeypatch, [_umm("C1-PODAAC")])

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "discover",
            "--limit",
            "1",
            "--list",
            "array",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "C1-PODAAC" in result.output

    state = load_state(state_path)
    assert len(state.collections) == 1
    assert state.run_meta.get("sampling_mode") == "limit=1"


def test_discover_skipped_flag_no_longer_accepted(tmp_path: Path, monkeypatch) -> None:
    from vzc.__main__ import cli

    _patch_search(monkeypatch, [_umm("C1-PODAAC")])

    runner = CliRunner()
    result = runner.invoke(cli, ["discover", "--limit", "1", "--skipped", "--dry-run"])
    assert result.exit_code != 0
    assert "no such option" in result.output.lower() or "--skipped" in result.output


def test_attempt_no_cache_only_flag_anymore() -> None:
    """``--cache-only`` was removed; --access external is cache-only by definition."""
    from vzc.__main__ import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["attempt", "--cache-only"])
    assert result.exit_code != 0
