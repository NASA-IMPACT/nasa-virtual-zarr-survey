"""Tests for vzc.render._intros."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from vzc.render._intros import (
    FeatureIntroduction,
    FeatureIntroductionsError,
    load_introductions,
)


def test_load_valid_introductions(tmp_path: Path) -> None:
    p = tmp_path / "intros.toml"
    p.write_text(
        "[has_datatree]\n"
        'phases = ["datatree"]\n'
        'first_in_vz = "2.0.0"\n'
        'introduced = "2026-03-15"\n'
        'description = "ManifestStore.to_virtual_datatree()"\n'
        "\n"
        "[has_fits_parser]\n"
        'phases = ["parse"]\n'
        'first_in_vz = "1.4.0"\n'
        'introduced = "2026-01-20"\n'
        'description = "FITSParser added."\n'
    )
    intros = load_introductions(p)
    assert isinstance(intros[0], FeatureIntroduction)
    assert [i.key for i in intros] == ["has_fits_parser", "has_datatree"]
    assert intros[0].introduced == date(2026, 1, 20)
    assert intros[1].phases == ["datatree"]


def test_missing_phases_rejected(tmp_path: Path) -> None:
    p = tmp_path / "intros.toml"
    p.write_text(
        '[bad]\nfirst_in_vz = "1.0.0"\nintroduced = "2026-01-01"\ndescription = "x"\n'
    )
    with pytest.raises(FeatureIntroductionsError, match="phases"):
        load_introductions(p)


def test_unknown_phase_rejected(tmp_path: Path) -> None:
    p = tmp_path / "intros.toml"
    p.write_text(
        "[bad]\n"
        'phases = ["bogus"]\n'
        'first_in_vz = "1.0.0"\n'
        'introduced = "2026-01-01"\n'
        'description = "x"\n'
    )
    with pytest.raises(FeatureIntroductionsError, match="bogus"):
        load_introductions(p)


def test_invalid_introduced_date_rejected(tmp_path: Path) -> None:
    p = tmp_path / "intros.toml"
    p.write_text(
        "[bad]\n"
        'phases = ["parse"]\n'
        'first_in_vz = "1.0.0"\n'
        'introduced = "not-a-date"\n'
        'description = "x"\n'
    )
    with pytest.raises(FeatureIntroductionsError, match="introduced"):
        load_introductions(p)


def test_empty_file_returns_empty_list(tmp_path: Path) -> None:
    p = tmp_path / "intros.toml"
    p.write_text("")
    assert load_introductions(p) == []


def test_missing_file_returns_empty_list(tmp_path: Path) -> None:
    """Missing file is fine -- feature annotations are optional."""
    assert load_introductions(tmp_path / "nonexistent.toml") == []
