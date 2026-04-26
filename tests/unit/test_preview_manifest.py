"""Tests for nasa_virtual_zarr_survey.preview_manifest."""

from __future__ import annotations

from pathlib import Path

import pytest

from nasa_virtual_zarr_survey.preview_manifest import (
    PreviewManifest,
    PreviewManifestError,
    load_manifest,
)


def test_valid_manifest(tmp_path: Path) -> None:
    text = """
snapshot_date = "2026-04-26"
label = "variable-chunking"
description = "Coordinated branches"
pypi_freeze_date = "2026-04-26"

[git_overrides]
virtualizarr = { url = "https://github.com/zarr-developers/VirtualiZarr", rev = "abc123de" }
xarray = { url = "https://github.com/pydata/xarray", rev = "deadbeef" }
"""
    p = tmp_path / "preview.toml"
    p.write_text(text)
    m = load_manifest(p)
    assert isinstance(m, PreviewManifest)
    assert m.snapshot_date == "2026-04-26"
    assert m.label == "variable-chunking"
    assert m.pypi_freeze_date == "2026-04-26"
    assert m.git_overrides == {
        "virtualizarr": {
            "url": "https://github.com/zarr-developers/VirtualiZarr",
            "rev": "abc123de",
        },
        "xarray": {
            "url": "https://github.com/pydata/xarray",
            "rev": "deadbeef",
        },
    }


def test_missing_snapshot_date_rejected(tmp_path: Path) -> None:
    p = tmp_path / "preview.toml"
    p.write_text('label = "x"\n[git_overrides]\nvz = {url="u", rev="abc1234"}\n')
    with pytest.raises(PreviewManifestError, match="snapshot_date"):
        load_manifest(p)


def test_missing_label_rejected(tmp_path: Path) -> None:
    p = tmp_path / "preview.toml"
    p.write_text(
        'snapshot_date = "2026-04-26"\n[git_overrides]\nvz = {url="u", rev="abc1234"}\n'
    )
    with pytest.raises(PreviewManifestError, match="label"):
        load_manifest(p)


def test_branch_name_in_rev_rejected(tmp_path: Path) -> None:
    text = """
snapshot_date = "2026-04-26"
label = "x"
[git_overrides]
vz = { url = "https://example", rev = "main" }
"""
    p = tmp_path / "preview.toml"
    p.write_text(text)
    with pytest.raises(PreviewManifestError, match="rev"):
        load_manifest(p)


def test_label_filename_unsafe_rejected(tmp_path: Path) -> None:
    text = """
snapshot_date = "2026-04-26"
label = "Has Spaces"
[git_overrides]
vz = { url = "u", rev = "abc1234" }
"""
    p = tmp_path / "preview.toml"
    p.write_text(text)
    with pytest.raises(PreviewManifestError, match="label"):
        load_manifest(p)


def test_pypi_freeze_date_defaults_to_snapshot_date(tmp_path: Path) -> None:
    text = """
snapshot_date = "2026-04-26"
label = "x"
[git_overrides]
vz = { url = "u", rev = "abc1234" }
"""
    p = tmp_path / "preview.toml"
    p.write_text(text)
    m = load_manifest(p)
    assert m.pypi_freeze_date == "2026-04-26"


def test_empty_overrides_rejected(tmp_path: Path) -> None:
    p = tmp_path / "preview.toml"
    p.write_text('snapshot_date = "2026-04-26"\nlabel = "x"\n[git_overrides]\n')
    with pytest.raises(PreviewManifestError, match="git_overrides"):
        load_manifest(p)


def test_invalid_snapshot_date_rejected(tmp_path: Path) -> None:
    p = tmp_path / "preview.toml"
    p.write_text(
        'snapshot_date = "not-a-date"\nlabel = "x"\n'
        '[git_overrides]\nvz = {url="u", rev="abc1234"}\n'
    )
    with pytest.raises(PreviewManifestError, match="snapshot_date"):
        load_manifest(p)
