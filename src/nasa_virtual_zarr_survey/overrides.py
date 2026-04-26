"""Per-collection overrides for parsers and to_virtual_* calls.

The override file is a checked-in TOML keyed by CMR collection concept ID.
At runtime, attempt.py loads the registry once and looks up each row's
override before constructing the parser and calling to_virtual_dataset /
to_virtual_datatree.
"""

from __future__ import annotations

import inspect as _inspect
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nasa_virtual_zarr_survey.formats import FormatFamily

CONCEPT_ID_RE = re.compile(r"^C\d+-[A-Z0-9]+$")

ALLOWED_SUBKEYS = {
    "parser",
    "dataset",
    "datatree",
    "skip_dataset",
    "skip_datatree",
    "notes",
}


class OverrideError(ValueError):
    """Raised when the override file is malformed."""


@dataclass(frozen=True)
class CollectionOverride:
    """Overrides applied to one collection's attempt.

    All fields default to no-op so callers can request the default for an
    unmapped collection without branching.
    """

    parser_kwargs: Mapping[str, Any] = field(default_factory=dict)
    dataset_kwargs: Mapping[str, Any] = field(default_factory=dict)
    datatree_kwargs: Mapping[str, Any] = field(default_factory=dict)
    skip_dataset: bool = False
    skip_datatree: bool = False
    notes: str | None = None

    def is_empty(self) -> bool:
        return (
            not self.parser_kwargs
            and not self.dataset_kwargs
            and not self.datatree_kwargs
            and not self.skip_dataset
            and not self.skip_datatree
        )


@dataclass(frozen=True)
class OverrideRegistry:
    """Read-only mapping of concept_id -> CollectionOverride."""

    _by_id: Mapping[str, CollectionOverride]

    def for_collection(self, concept_id: str) -> CollectionOverride:
        return self._by_id.get(concept_id, CollectionOverride())

    @classmethod
    def empty(cls) -> "OverrideRegistry":
        """Return a registry with no overrides — used by --no-overrides."""
        return cls(_by_id={})

    @classmethod
    def from_toml(cls, path: str | Path) -> "OverrideRegistry":
        """Load and structurally validate an override TOML file.

        Returns an empty registry if the file does not exist. Raises
        OverrideError on malformed entries (bad concept-id key, unknown
        sub-key, contradictory skip flag, missing notes).
        """
        p = Path(path)
        if not p.exists():
            return cls(_by_id={})

        with p.open("rb") as f:
            raw = tomllib.load(f)

        by_id: dict[str, CollectionOverride] = {}
        for key, body in raw.items():
            if not CONCEPT_ID_RE.match(key):
                raise OverrideError(
                    f"{p}: top-level key {key!r} is not a CMR concept ID "
                    f"(expected pattern '^C\\d+-[A-Z0-9]+$')"
                )
            if not isinstance(body, dict):
                raise OverrideError(
                    f"{p}: entry for {key} must be a table, got {type(body).__name__}"
                )

            extra = set(body) - ALLOWED_SUBKEYS
            if extra:
                raise OverrideError(
                    f"{p}: entry {key} has unknown sub-key(s) {sorted(extra)!r}; "
                    f"allowed: {sorted(ALLOWED_SUBKEYS)!r}"
                )

            parser_kw = body.get("parser", {}) or {}
            ds_kw = body.get("dataset", {}) or {}
            dt_kw = body.get("datatree", {}) or {}
            skip_ds = bool(body.get("skip_dataset", False))
            skip_dt = bool(body.get("skip_datatree", False))

            for label, kw in (
                ("parser", parser_kw),
                ("dataset", ds_kw),
                ("datatree", dt_kw),
            ):
                if not isinstance(kw, dict):
                    raise OverrideError(
                        f"{p}: entry {key}.{label} must be an inline table, "
                        f"got {type(kw).__name__}"
                    )

            if skip_ds and ds_kw:
                raise OverrideError(
                    f"{p}: entry {key} sets skip_dataset=true and "
                    f"dataset={ds_kw!r}; remove one"
                )
            if skip_dt and dt_kw:
                raise OverrideError(
                    f"{p}: entry {key} sets skip_datatree=true and "
                    f"datatree={dt_kw!r}; remove one"
                )

            ov = CollectionOverride(
                parser_kwargs=parser_kw,
                dataset_kwargs=ds_kw,
                datatree_kwargs=dt_kw,
                skip_dataset=skip_ds,
                skip_datatree=skip_dt,
                notes=body.get("notes"),
            )
            if not ov.is_empty() and not ov.notes:
                raise OverrideError(
                    f"{p}: entry {key} is non-empty but has no `notes` field; "
                    "every override must include a one-line rationale"
                )
            by_id[key] = ov

        return cls(_by_id=by_id)

    def validate(self, *, format_for: Mapping[str, FormatFamily]) -> None:
        """Cross-check every override against parser/method signatures.

        `format_for` maps each concept_id to its format family. Overrides
        targeting unknown collections are skipped (the caller decides
        whether that is fatal).
        """
        ds_params = _to_virtual_dataset_params()
        dt_params = _to_virtual_datatree_params()

        for cid, ov in self._by_id.items():
            if cid not in format_for:
                continue

            family = format_for[cid]
            cls = _parser_class_for(family)
            if cls is None and ov.parser_kwargs:
                raise OverrideError(
                    f"{cid}: parser kwargs {dict(ov.parser_kwargs)!r} given "
                    f"but no parser is registered for format family "
                    f"{family.value}"
                )
            if cls is not None:
                # _accepted_kwargs(cls) introspects __init__ via inspect.signature
                # and excludes self. Avoids mypy's "unsound __init__ access" warning.
                allowed = _accepted_kwargs(cls)
                extra = set(ov.parser_kwargs) - allowed
                if extra:
                    raise OverrideError(
                        f"{cid}: parser kwarg(s) {sorted(extra)!r} not "
                        f"accepted by {cls.__name__} "
                        f"(allowed: {sorted(allowed)!r})"
                    )

            extra_ds = set(ov.dataset_kwargs) - ds_params
            if extra_ds:
                raise OverrideError(
                    f"{cid}: dataset kwarg(s) {sorted(extra_ds)!r} not "
                    f"accepted by ManifestStore.to_virtual_dataset "
                    f"(allowed: {sorted(ds_params)!r})"
                )
            extra_dt = set(ov.datatree_kwargs) - dt_params
            if extra_dt:
                raise OverrideError(
                    f"{cid}: datatree kwarg(s) {sorted(extra_dt)!r} not "
                    f"accepted by ManifestStore.to_virtual_datatree "
                    f"(allowed: {sorted(dt_params)!r})"
                )


def apply_to_parser(parser_cls: type, kwargs: Mapping[str, Any]) -> Any:
    """Construct a parser with override kwargs (or no kwargs if empty)."""
    return parser_cls(**dict(kwargs))


def apply_to_dataset_call(manifest_store: Any, kwargs: Mapping[str, Any]) -> Any:
    return manifest_store.to_virtual_dataset(**dict(kwargs))


def apply_to_datatree_call(manifest_store: Any, kwargs: Mapping[str, Any]) -> Any:
    return manifest_store.to_virtual_datatree(**dict(kwargs))


def _accepted_kwargs(callable_obj: Any) -> set[str]:
    sig = _inspect.signature(callable_obj)
    return {
        p.name
        for p in sig.parameters.values()
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        and p.name not in ("self", "cls")
    }


def _parser_class_for(family: FormatFamily) -> type | None:
    # Local import to avoid circular import (attempt -> overrides -> attempt).
    from nasa_virtual_zarr_survey.attempt import dispatch_parser

    inst = dispatch_parser(family)
    return type(inst) if inst is not None else None


def _to_virtual_dataset_params() -> set[str]:
    from virtualizarr.manifests import ManifestStore

    return _accepted_kwargs(ManifestStore.to_virtual_dataset)


def _to_virtual_datatree_params() -> set[str]:
    from virtualizarr.manifests import ManifestStore

    return _accepted_kwargs(ManifestStore.to_virtual_datatree)
