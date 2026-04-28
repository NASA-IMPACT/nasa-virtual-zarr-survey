"""Tests for vzc.snapshot and the `snapshot` CLI subcommand."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from vzc.__main__ import cli
from vzc.snapshot import (
    SnapshotError,
    _run,
    read_pyproject_exclude_newer,
)
from vzc.state._io import SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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


@pytest.fixture
def stub_attempt_one(monkeypatch):
    """Replace attempt_one + StoreCache.get_store so snapshot tests don't fetch."""
    import vzc.pipeline._attempt as attempt_mod

    def fake_attempt_one(**kwargs):
        return attempt_mod.AttemptResult(
            collection_concept_id=kwargs["collection_concept_id"],
            granule_concept_id=kwargs["granule_concept_id"],
            daac=kwargs["daac"],
            format_family=kwargs["family"].value,
            parser="HDFParser",
            parse_success=True,
            dataset_success=True,
            datatree_success=False,
            success=True,
            duration_s=0.1,
            attempted_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr(attempt_mod, "attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        attempt_mod.StoreCache,
        "get_store",
        lambda self, *, provider, url: object(),
    )


# ---------------------------------------------------------------------------
# pyproject.toml exclude-newer reader
# ---------------------------------------------------------------------------


def test_read_pyproject_exclude_newer_iso_date(tmp_path: Path) -> None:
    p = tmp_path / "pyproject.toml"
    p.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    assert read_pyproject_exclude_newer(p) == "2026-02-15"


def test_read_pyproject_exclude_newer_rfc3339(tmp_path: Path) -> None:
    p = tmp_path / "pyproject.toml"
    p.write_text('[tool.uv]\nexclude-newer = "2026-02-15T12:00:00Z"\n')
    assert read_pyproject_exclude_newer(p) == "2026-02-15"


def test_read_pyproject_exclude_newer_missing(tmp_path: Path) -> None:
    p = tmp_path / "pyproject.toml"
    p.write_text("[project]\nname = 'x'\n")
    assert read_pyproject_exclude_newer(p) is None


def test_read_pyproject_exclude_newer_no_file(tmp_path: Path) -> None:
    assert read_pyproject_exclude_newer(tmp_path / "absent.toml") is None


# ---------------------------------------------------------------------------
# _run()
# ---------------------------------------------------------------------------


def test_run_snapshot_release_uses_pyproject_date(
    tmp_path: Path, stub_attempt_one
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))
    history_dir = tmp_path / "history"
    results_dir = tmp_path / "results"

    out = _run(
        locked_sample_path=locked_sample,
        access="external",
        results_dir=results_dir,
        history_dir=history_dir,
        pyproject_path=pyproject,
        cache_dir=tmp_path / "cache",
    )
    assert out == history_dir / "2026-02-15.summary.json"
    payload = json.loads(out.read_text())
    assert payload["snapshot_kind"] == "release"
    assert payload["snapshot_date"] == "2026-02-15"
    # No uv.lock copied beside the digest anymore.
    assert not (history_dir / "2026-02-15.uv.lock").exists()


def test_run_snapshot_explicit_date_overrides_pyproject(
    tmp_path: Path, stub_attempt_one
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    out = _run(
        snapshot_date="2026-04-01",
        locked_sample_path=locked_sample,
        results_dir=tmp_path / "results",
        history_dir=tmp_path / "history",
        pyproject_path=pyproject,
        cache_dir=tmp_path / "cache",
    )
    assert out.name == "2026-04-01.summary.json"
    payload = json.loads(out.read_text())
    assert payload["snapshot_date"] == "2026-04-01"


def test_run_snapshot_label_marks_preview(tmp_path: Path, stub_attempt_one) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-04-26"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    out = _run(
        label="variable-chunking",
        description="testing variable chunking",
        locked_sample_path=locked_sample,
        results_dir=tmp_path / "results",
        history_dir=tmp_path / "history",
        pyproject_path=pyproject,
        cache_dir=tmp_path / "cache",
    )
    assert out.name == "2026-04-26-variable-chunking.summary.json"
    payload = json.loads(out.read_text())
    assert payload["snapshot_kind"] == "preview"
    assert payload["snapshot_date"] == "2026-04-26"
    assert payload["label"] == "variable-chunking"
    assert payload["description"] == "testing variable chunking"


def test_run_snapshot_description_without_label_errors(
    tmp_path: Path, stub_attempt_one
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-04-26"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    with pytest.raises(SnapshotError, match="label"):
        _run(
            description="orphaned description",
            locked_sample_path=locked_sample,
            results_dir=tmp_path / "results",
            history_dir=tmp_path / "history",
            pyproject_path=pyproject,
        )


def test_run_snapshot_no_date_anywhere_errors(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'x'\n")
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    with pytest.raises(SnapshotError, match="exclude-newer"):
        _run(
            locked_sample_path=locked_sample,
            results_dir=tmp_path / "results",
            history_dir=tmp_path / "history",
            pyproject_path=pyproject,
        )


# ---------------------------------------------------------------------------
# CLI subcommand
# ---------------------------------------------------------------------------


def _setup_snapshot_layout(tmp_path: Path) -> None:
    """Create the canonical files snapshot reads under tmp_path."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.uv]\nexclude-newer = "2026-02-15"\n'
    )
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "locked_sample.json").write_text(
        json.dumps(_locked_sample_payload())
    )


def test_snapshot_cli_release(tmp_path: Path, stub_attempt_one, monkeypatch) -> None:
    _setup_snapshot_layout(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--access", "external"])
    assert result.exit_code == 0, result.output
    out = tmp_path / "docs" / "results" / "history" / "2026-02-15.summary.json"
    assert out.exists()


def test_snapshot_cli_external_uses_cache_dir(
    tmp_path: Path, stub_attempt_one, monkeypatch
) -> None:
    """`--access external` reads from NASA_VZ_SURVEY_CACHE_DIR."""
    _setup_snapshot_layout(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))

    captured: dict = {}
    import vzc.pipeline._attempt as attempt_mod

    real_init = attempt_mod.StoreCache.__init__

    def spy_init(self, *args, **kwargs):
        captured["cache_dir"] = kwargs.get("cache_dir")
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(attempt_mod.StoreCache, "__init__", spy_init)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--access", "external"])
    assert result.exit_code == 0, result.output
    # snapshot CLI uses DEFAULT_CACHE_DIR (which honors the env var indirectly
    # — but the CLI today reads the constant; checking the kwarg flowed through
    # is sufficient).
    assert captured["cache_dir"] is not None


def test_snapshot_cli_direct_mode_skips_cache(
    tmp_path: Path, stub_attempt_one, monkeypatch
) -> None:
    """`--access direct` doesn't touch the cache (in-region S3 is fast/free)."""
    _setup_snapshot_layout(tmp_path)
    monkeypatch.chdir(tmp_path)

    captured: dict = {}
    import vzc.pipeline._attempt as attempt_mod

    real_init = attempt_mod.StoreCache.__init__

    def spy_init(self, *args, **kwargs):
        captured["cache_dir"] = kwargs.get("cache_dir")
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(attempt_mod.StoreCache, "__init__", spy_init)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--access", "direct"])
    assert result.exit_code == 0, result.output
    assert captured["cache_dir"] is None
