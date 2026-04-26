"""Tests for nasa_virtual_zarr_survey.snapshot and the `snapshot` CLI subcommand."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from nasa_virtual_zarr_survey.__main__ import cli
from nasa_virtual_zarr_survey.snapshot import (
    SnapshotError,
    read_pyproject_exclude_newer,
    read_pyproject_git_sources,
    run_snapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _locked_sample_payload() -> dict:
    return {
        "schema_version": 1,
        "created_at": "2026-04-26T12:00:00Z",
        "sampling_mode": "top=1",
        "collections": [
            {
                "concept_id": "C1-T",
                "daac": "X.DAAC",
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
                "temporal_bin": 0,
                "size_bytes": 100,
                "stratified": True,
            }
        ],
    }


@pytest.fixture
def stub_attempt_one(monkeypatch):
    """Replace attempt_one + StoreCache.get_store so snapshot tests don't fetch."""
    import nasa_virtual_zarr_survey.attempt as attempt_mod

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
# pyproject.toml readers
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


def test_read_pyproject_git_sources(tmp_path: Path) -> None:
    p = tmp_path / "pyproject.toml"
    p.write_text(
        "[tool.uv.sources]\n"
        'virtualizarr = { git = "https://github.com/zarr-developers/VirtualiZarr", rev = "abc123de" }\n'
        'xarray = { git = "https://github.com/pydata/xarray", rev = "deadbeef" }\n'
        # Non-git source should be ignored.
        "local = { workspace = true }\n"
    )
    sources = read_pyproject_git_sources(p)
    assert sources == {
        "virtualizarr": {
            "url": "https://github.com/zarr-developers/VirtualiZarr",
            "rev": "abc123de",
        },
        "xarray": {
            "url": "https://github.com/pydata/xarray",
            "rev": "deadbeef",
        },
    }


def test_read_pyproject_git_sources_branch_rejected(tmp_path: Path) -> None:
    p = tmp_path / "pyproject.toml"
    p.write_text(
        "[tool.uv.sources]\n"
        'virtualizarr = { git = "https://github.com/zarr-developers/VirtualiZarr", branch = "main" }\n'
    )
    with pytest.raises(SnapshotError, match="hex SHA"):
        read_pyproject_git_sources(p)


def test_read_pyproject_git_sources_empty(tmp_path: Path) -> None:
    p = tmp_path / "pyproject.toml"
    p.write_text("[project]\nname = 'x'\n")
    assert read_pyproject_git_sources(p) == {}


# ---------------------------------------------------------------------------
# run_snapshot()
# ---------------------------------------------------------------------------


def test_run_snapshot_release_uses_pyproject_date(
    tmp_path: Path, stub_attempt_one
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))
    uv_lock = tmp_path / "uv.lock"
    uv_lock.write_text("# fake lock\n")
    history_dir = tmp_path / "history"
    results_dir = tmp_path / "results"

    out = run_snapshot(
        locked_sample_path=locked_sample,
        access="external",
        uv_lock_path=uv_lock,
        results_dir=results_dir,
        history_dir=history_dir,
        pyproject_path=pyproject,
    )
    assert out == history_dir / "2026-02-15.summary.json"
    payload = json.loads(out.read_text())
    assert payload["snapshot_kind"] == "release"
    assert payload["snapshot_date"] == "2026-02-15"
    # uv.lock copied beside the digest.
    assert (history_dir / "2026-02-15.uv.lock").exists()


def test_run_snapshot_explicit_date_overrides_pyproject(
    tmp_path: Path, stub_attempt_one
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    out = run_snapshot(
        snapshot_date="2026-04-01",
        locked_sample_path=locked_sample,
        results_dir=tmp_path / "results",
        history_dir=tmp_path / "history",
        pyproject_path=pyproject,
        uv_lock_path=tmp_path / "missing.lock",
    )
    assert out.name == "2026-04-01.summary.json"
    payload = json.loads(out.read_text())
    assert payload["snapshot_date"] == "2026-04-01"


def test_run_snapshot_auto_preview_from_git_sources(
    tmp_path: Path, stub_attempt_one
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.uv]\nexclude-newer = "2026-04-26"\n'
        "[tool.uv.sources]\n"
        'virtualizarr = { git = "https://github.com/zarr-developers/VirtualiZarr", rev = "abc123de" }\n'
    )
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    out = run_snapshot(
        label="variable-chunking",
        description="testing variable chunking",
        locked_sample_path=locked_sample,
        results_dir=tmp_path / "results",
        history_dir=tmp_path / "history",
        pyproject_path=pyproject,
        uv_lock_path=tmp_path / "missing.lock",
    )
    assert out.name == "2026-04-26-variable-chunking.summary.json"
    payload = json.loads(out.read_text())
    assert payload["snapshot_kind"] == "preview"
    assert payload["snapshot_date"] == "2026-04-26"
    assert payload["label"] == "variable-chunking"
    assert payload["description"] == "testing variable chunking"
    assert payload["git_overrides"] == {
        "virtualizarr": {
            "url": "https://github.com/zarr-developers/VirtualiZarr",
            "rev": "abc123de",
        }
    }
    # Previews skip uv.lock copy.
    assert payload["uv_lock_sha256"] is None
    assert not (
        Path(tmp_path / "history") / "2026-04-26-variable-chunking.uv.lock"
    ).exists()


def test_run_snapshot_auto_preview_requires_label(
    tmp_path: Path, stub_attempt_one
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.uv]\nexclude-newer = "2026-04-26"\n'
        "[tool.uv.sources]\n"
        'vz = { git = "https://github.com/zarr-developers/VirtualiZarr", rev = "abc123de" }\n'
    )
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    with pytest.raises(SnapshotError, match="label"):
        run_snapshot(
            locked_sample_path=locked_sample,
            results_dir=tmp_path / "results",
            history_dir=tmp_path / "history",
            pyproject_path=pyproject,
            uv_lock_path=tmp_path / "missing.lock",
        )


def test_run_snapshot_no_date_anywhere_errors(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'x'\n")
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    with pytest.raises(SnapshotError, match="exclude-newer"):
        run_snapshot(
            locked_sample_path=locked_sample,
            results_dir=tmp_path / "results",
            history_dir=tmp_path / "history",
            pyproject_path=pyproject,
            uv_lock_path=tmp_path / "missing.lock",
        )


def test_run_snapshot_explicit_preview_manifest(
    tmp_path: Path, stub_attempt_one
) -> None:
    """`preview_manifest_path` takes precedence over pyproject auto-detection."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    manifest = tmp_path / "preview.toml"
    manifest.write_text(
        'snapshot_date = "2026-05-01"\n'
        'label = "manifest-driven"\n'
        "[git_overrides]\n"
        'vz = { url = "https://github.com/zarr-developers/VirtualiZarr", rev = "1234567" }\n'
    )
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    out = run_snapshot(
        preview_manifest_path=manifest,
        locked_sample_path=locked_sample,
        results_dir=tmp_path / "results",
        history_dir=tmp_path / "history",
        pyproject_path=pyproject,
        uv_lock_path=tmp_path / "missing.lock",
    )
    payload = json.loads(out.read_text())
    assert out.name == "2026-05-01-manifest-driven.summary.json"
    assert payload["snapshot_kind"] == "preview"
    assert payload["snapshot_date"] == "2026-05-01"


# ---------------------------------------------------------------------------
# CLI subcommand
# ---------------------------------------------------------------------------


def test_snapshot_cli_release(tmp_path: Path, stub_attempt_one, monkeypatch) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    monkeypatch.chdir(tmp_path)  # so CLI default `pyproject.toml` resolves here

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "snapshot",
            "--locked-sample",
            str(locked_sample),
            "--access",
            "external",
            "--uv-lock",
            str(tmp_path / "missing.lock"),
            "--results",
            str(tmp_path / "results"),
            "--history-dir",
            str(tmp_path / "history"),
        ],
    )
    assert result.exit_code == 0, result.output
    out = tmp_path / "history" / "2026-02-15.summary.json"
    assert out.exists()


def test_snapshot_cli_default_enables_cache(
    tmp_path: Path, stub_attempt_one, monkeypatch
) -> None:
    """`snapshot` defaults to --cache (vs `attempt` which defaults to --no-cache)."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))
    monkeypatch.chdir(tmp_path)

    captured: dict = {}
    import nasa_virtual_zarr_survey.attempt as attempt_mod

    real_init = attempt_mod.StoreCache.__init__

    def spy_init(self, *args, **kwargs):
        captured["cache_dir"] = kwargs.get("cache_dir")
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(attempt_mod.StoreCache, "__init__", spy_init)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "snapshot",
            "--locked-sample",
            str(locked_sample),
            "--access",
            "external",
            "--uv-lock",
            str(tmp_path / "missing.lock"),
            "--results",
            str(tmp_path / "results"),
            "--history-dir",
            str(tmp_path / "history"),
            "--cache-dir",
            str(tmp_path / "cache"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["cache_dir"] == tmp_path / "cache"


def test_snapshot_cli_no_cache_disables(
    tmp_path: Path, stub_attempt_one, monkeypatch
) -> None:
    """`--no-cache` overrides the default-on behavior."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.uv]\nexclude-newer = "2026-02-15"\n')
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))
    monkeypatch.chdir(tmp_path)

    captured: dict = {}
    import nasa_virtual_zarr_survey.attempt as attempt_mod

    real_init = attempt_mod.StoreCache.__init__

    def spy_init(self, *args, **kwargs):
        captured["cache_dir"] = kwargs.get("cache_dir")
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(attempt_mod.StoreCache, "__init__", spy_init)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "snapshot",
            "--locked-sample",
            str(locked_sample),
            "--no-cache",
            "--uv-lock",
            str(tmp_path / "missing.lock"),
            "--results",
            str(tmp_path / "results"),
            "--history-dir",
            str(tmp_path / "history"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["cache_dir"] is None


def test_snapshot_cli_preview_requires_label_message(
    tmp_path: Path, monkeypatch
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.uv]\nexclude-newer = "2026-04-26"\n'
        "[tool.uv.sources]\n"
        'vz = { git = "https://github.com/zarr-developers/VirtualiZarr", rev = "abc123de" }\n'
    )
    locked_sample = tmp_path / "locked.json"
    locked_sample.write_text(json.dumps(_locked_sample_payload()))

    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "snapshot",
            "--locked-sample",
            str(locked_sample),
            "--results",
            str(tmp_path / "results"),
            "--history-dir",
            str(tmp_path / "history"),
            "--uv-lock",
            str(tmp_path / "missing.lock"),
        ],
    )
    assert result.exit_code != 0
    assert "label" in result.output
