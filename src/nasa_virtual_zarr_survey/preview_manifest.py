"""Parse + validate config/snapshot_previews/<date>-<label>.toml."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path


class PreviewManifestError(ValueError):
    pass


_LABEL_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_REV_RE = re.compile(r"^[0-9a-f]{7,40}$")


@dataclass(frozen=True)
class PreviewManifest:
    snapshot_date: str
    label: str
    pypi_freeze_date: str
    git_overrides: dict[str, dict[str, str]]
    description: str = ""


def _require_iso_date(value: str, field_name: str) -> None:
    try:
        date.fromisoformat(value)
    except (ValueError, TypeError) as e:
        raise PreviewManifestError(
            f"{field_name} must be an ISO date (YYYY-MM-DD); got {value!r}"
        ) from e


def load_manifest(path: Path | str) -> PreviewManifest:
    raw = tomllib.loads(Path(path).read_text())

    snapshot_date = raw.get("snapshot_date")
    if not snapshot_date:
        raise PreviewManifestError("manifest is missing snapshot_date")
    _require_iso_date(snapshot_date, "snapshot_date")

    label = raw.get("label")
    if not label:
        raise PreviewManifestError("manifest is missing label")
    if not _LABEL_RE.match(label):
        raise PreviewManifestError(
            f"label must match {_LABEL_RE.pattern}; got {label!r}"
        )

    pypi_freeze_date = raw.get("pypi_freeze_date") or snapshot_date
    _require_iso_date(pypi_freeze_date, "pypi_freeze_date")

    description = raw.get("description", "")

    overrides = raw.get("git_overrides")
    if not overrides:
        raise PreviewManifestError(
            "manifest must include at least one [git_overrides] entry"
        )

    validated: dict[str, dict[str, str]] = {}
    for name, entry in overrides.items():
        url = entry.get("url")
        rev = entry.get("rev")
        if not url:
            raise PreviewManifestError(f"git_overrides.{name}.url is required")
        if not rev:
            raise PreviewManifestError(f"git_overrides.{name}.rev is required")
        if not _REV_RE.match(rev):
            raise PreviewManifestError(
                f"git_overrides.{name}.rev must be a hex SHA "
                f"(7-40 chars, [0-9a-f]); got {rev!r}. "
                "Branch names and tags are rejected for reproducibility."
            )
        validated[name] = {"url": url, "rev": rev}

    return PreviewManifest(
        snapshot_date=snapshot_date,
        label=label,
        pypi_freeze_date=pypi_freeze_date,
        description=description,
        git_overrides=validated,
    )
