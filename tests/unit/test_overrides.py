"""Unit tests for nasa_virtual_zarr_survey.overrides."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from nasa_virtual_zarr_survey.formats import FormatFamily
from nasa_virtual_zarr_survey.overrides import (
    CollectionOverride,
    OverrideError,
    OverrideRegistry,
    apply_to_dataset_call,
    apply_to_datatree_call,
    apply_to_parser,
)


def _write(tmp_path: Path, contents: str) -> Path:
    path = tmp_path / "overrides.toml"
    path.write_text(textwrap.dedent(contents))
    return path


# ---------------------------------------------------------------------------
# Defaults / lookup
# ---------------------------------------------------------------------------


def test_empty_registry_returns_default_override() -> None:
    reg = OverrideRegistry(_by_id={})
    ov = reg.for_collection("C123-DAAC")
    assert ov == CollectionOverride()
    assert ov.parser_kwargs == {}
    assert ov.dataset_kwargs == {}
    assert ov.datatree_kwargs == {}
    assert ov.skip_dataset is False
    assert ov.skip_datatree is False
    assert ov.is_empty()


def test_from_toml_missing_file_returns_empty_registry(tmp_path: Path) -> None:
    reg = OverrideRegistry.from_toml(tmp_path / "absent.toml")
    assert reg.for_collection("C1-DAAC").is_empty()


# ---------------------------------------------------------------------------
# TOML loading
# ---------------------------------------------------------------------------


def test_from_toml_loads_well_formed_entry(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        [C1234567890-POCLOUD]
        parser = { group = "science" }
        dataset = { loadable_variables = [] }
        notes = "test entry"
        """,
    )
    reg = OverrideRegistry.from_toml(path)
    ov = reg.for_collection("C1234567890-POCLOUD")
    assert ov.parser_kwargs == {"group": "science"}
    assert ov.dataset_kwargs == {"loadable_variables": []}
    assert ov.notes == "test entry"


def test_from_toml_rejects_malformed_concept_id(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        [not-a-concept-id]
        parser = {}
        notes = "bad key"
        """,
    )
    with pytest.raises(OverrideError, match="not-a-concept-id"):
        OverrideRegistry.from_toml(path)


def test_from_toml_rejects_unknown_subkey(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        [C1-POCLOUD]
        parser = {}
        wat = "no such field"
        notes = "ok"
        """,
    )
    with pytest.raises(OverrideError, match="wat"):
        OverrideRegistry.from_toml(path)


def test_from_toml_rejects_skip_dataset_with_dataset_kwargs(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        [C1-POCLOUD]
        skip_dataset = true
        dataset = { loadable_variables = [] }
        notes = "contradictory"
        """,
    )
    with pytest.raises(OverrideError, match="skip_dataset"):
        OverrideRegistry.from_toml(path)


def test_from_toml_rejects_skip_datatree_with_datatree_kwargs(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        [C1-POCLOUD]
        skip_datatree = true
        datatree = { loadable_variables = [] }
        notes = "contradictory"
        """,
    )
    with pytest.raises(OverrideError, match="skip_datatree"):
        OverrideRegistry.from_toml(path)


def test_from_toml_requires_notes_on_nonempty_entry(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        [C1-POCLOUD]
        parser = { group = "science" }
        """,
    )
    with pytest.raises(OverrideError, match="notes"):
        OverrideRegistry.from_toml(path)


def test_from_toml_allows_skip_only_entry(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        [C1-POCLOUD]
        skip_dataset = true
        notes = "datatree-only collection"
        """,
    )
    reg = OverrideRegistry.from_toml(path)
    ov = reg.for_collection("C1-POCLOUD")
    assert ov.skip_dataset is True
    assert ov.notes == "datatree-only collection"


# ---------------------------------------------------------------------------
# Signature-aware validation
# ---------------------------------------------------------------------------


def test_validate_accepts_known_parser_kwarg() -> None:
    reg = OverrideRegistry(
        _by_id={
            "C1-POCLOUD": CollectionOverride(
                parser_kwargs={"group": "science", "drop_variables": ["status"]},
                notes="ok",
            )
        }
    )
    reg.validate(format_for={"C1-POCLOUD": FormatFamily.HDF5})


def test_validate_rejects_unknown_parser_kwarg() -> None:
    reg = OverrideRegistry(
        _by_id={
            "C1-POCLOUD": CollectionOverride(
                parser_kwargs={"groups": "science"},  # plural typo
                notes="typo",
            )
        }
    )
    with pytest.raises(OverrideError, match="groups"):
        reg.validate(format_for={"C1-POCLOUD": FormatFamily.HDF5})


def test_validate_rejects_drop_variables_for_zarr_parser() -> None:
    # ZarrParser uses skip_variables, not drop_variables.
    reg = OverrideRegistry(
        _by_id={
            "C1-POCLOUD": CollectionOverride(
                parser_kwargs={"drop_variables": ["x"]},
                notes="wrong kwarg name for zarr",
            )
        }
    )
    with pytest.raises(OverrideError, match="drop_variables"):
        reg.validate(format_for={"C1-POCLOUD": FormatFamily.ZARR})


def test_validate_dataset_kwargs_against_to_virtual_dataset() -> None:
    reg = OverrideRegistry(
        _by_id={
            "C1-POCLOUD": CollectionOverride(
                dataset_kwargs={"loadable_variables": ["t"]},
                notes="ok",
            )
        }
    )
    reg.validate(format_for={"C1-POCLOUD": FormatFamily.HDF5})

    bad = OverrideRegistry(
        _by_id={
            "C1-POCLOUD": CollectionOverride(
                dataset_kwargs={"loadable_vars": ["t"]},  # typo
                notes="bad",
            )
        }
    )
    with pytest.raises(OverrideError, match="loadable_vars"):
        bad.validate(format_for={"C1-POCLOUD": FormatFamily.HDF5})


def test_validate_skips_unknown_collections() -> None:
    """Overrides for collections not in format_for are silently skipped."""
    reg = OverrideRegistry(
        _by_id={
            "C1-POCLOUD": CollectionOverride(
                parser_kwargs={"groups": "science"},  # would fail validation
                notes="targets a collection not in the survey",
            )
        }
    )
    # Empty format_for means C1-POCLOUD isn't checked.
    reg.validate(format_for={})


# ---------------------------------------------------------------------------
# Apply helpers
# ---------------------------------------------------------------------------


def test_apply_to_parser_passes_kwargs() -> None:
    class Fake:
        def __init__(
            self,
            group: str | None = None,
            drop_variables: list[str] | None = None,
        ) -> None:
            self.group = group
            self.drop_variables = drop_variables

    inst = apply_to_parser(Fake, {"group": "science", "drop_variables": ["x"]})
    assert inst.group == "science"
    assert inst.drop_variables == ["x"]


def test_apply_to_parser_with_no_kwargs_returns_default_instance() -> None:
    class Fake:
        def __init__(self, group: str | None = None) -> None:
            self.group = group

    inst = apply_to_parser(Fake, {})
    assert inst.group is None


def test_apply_to_dataset_call_invokes_method() -> None:
    captured: dict = {}

    class FakeManifest:
        def to_virtual_dataset(self, **kw):
            captured["kwargs"] = kw
            return "ds-result"

        def to_virtual_datatree(self, **kw):
            captured["kwargs"] = kw
            return "dt-result"

    manifest = FakeManifest()
    out = apply_to_dataset_call(manifest, {"loadable_variables": ["t"]})
    assert out == "ds-result"
    assert captured["kwargs"] == {"loadable_variables": ["t"]}

    out = apply_to_datatree_call(manifest, {})
    assert out == "dt-result"
    assert captured["kwargs"] == {}
