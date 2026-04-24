"""JSON digest format for survey results.

A small JSON file with everything `render_report` needs, so the docs site can
be regenerated without access to the original DuckDB / Parquet artifacts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from nasa_virtual_zarr_survey.cubability import CubabilityResult, CubabilityVerdict
from nasa_virtual_zarr_survey.types import VerdictRow

SCHEMA_VERSION = 3


def dump_summary(
    path: Path | str,
    *,
    verdicts: list[VerdictRow],
    parse_taxonomy: dict[str, tuple[int, int]],
    dataset_taxonomy: dict[str, tuple[int, int]],
    datatree_taxonomy: dict[str, tuple[int, int]],
    cubability_results: dict[str, CubabilityResult],
    other_parse_errors: list[tuple[int, str, str]],
    other_dataset_errors: list[tuple[int, str, str]],
    other_datatree_errors: list[tuple[int, str, str]],
    survey_tool_version: str,
    virtualizarr_version: str | None = None,
    zarr_version: str | None = None,
    xarray_version: str | None = None,
    sampling_mode: str | None = None,
    generated_at: str | None = None,
) -> Path:
    """Serialize everything the report needs to a compact JSON file."""
    cube_serialized = {
        cid: {
            "verdict": r.verdict.value,
            "reason": r.reason,
            "concat_dim": r.concat_dim,
        }
        for cid, r in cubability_results.items()
    }
    # TypedDicts are plain dicts at runtime; serialize datetimes if present.
    verdict_rows = [dict(v) for v in verdicts]
    for row in verdict_rows:
        for key, value in list(row.items()):
            if isinstance(value, datetime):
                row[key] = value.isoformat()

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "survey_tool_version": survey_tool_version,
        "virtualizarr_version": virtualizarr_version,
        "zarr_version": zarr_version,
        "xarray_version": xarray_version,
        "sampling_mode": sampling_mode,
        "verdicts": verdict_rows,
        "parse_taxonomy": {k: [v[0], v[1]] for k, v in parse_taxonomy.items()},
        "dataset_taxonomy": {k: [v[0], v[1]] for k, v in dataset_taxonomy.items()},
        "datatree_taxonomy": {k: [v[0], v[1]] for k, v in datatree_taxonomy.items()},
        "cubability_results": cube_serialized,
        "other_parse_errors": [list(e) for e in other_parse_errors],
        "other_dataset_errors": [list(e) for e in other_dataset_errors],
        "other_datatree_errors": [list(e) for e in other_datatree_errors],
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False))
    return path


@dataclass
class LoadedSummary:
    verdicts: list[VerdictRow]
    parse_taxonomy: dict[str, tuple[int, int]]
    dataset_taxonomy: dict[str, tuple[int, int]]
    datatree_taxonomy: dict[str, tuple[int, int]]
    cubability_results: dict[str, CubabilityResult]
    other_parse_errors: list[tuple[int, str, str]]
    other_dataset_errors: list[tuple[int, str, str]]
    other_datatree_errors: list[tuple[int, str, str]]
    generated_at: str
    survey_tool_version: str
    virtualizarr_version: str | None = None
    zarr_version: str | None = None
    xarray_version: str | None = None
    sampling_mode: str | None = None


def load_summary(path: Path | str) -> LoadedSummary:
    """Parse a summary JSON back into the typed structures used by render_report.

    Supports schema versions 1, 2, and 3.  Older versions are migrated by
    synthesizing safe defaults for newer fields:

    - v1 -> v2: empty datatree structures, ``datatree_verdict = "not_attempted"``.
    - v2 -> v3: dependency versions and sampling mode default to ``None``.
    """
    data = json.loads(Path(path).read_text())
    version = data.get("schema_version")
    if version not in (1, 2, SCHEMA_VERSION):
        raise ValueError(
            f"Unsupported schema_version: {version!r} (expected {SCHEMA_VERSION})"
        )

    verdicts = cast(list[VerdictRow], data["verdicts"])

    # v1 -> v2 migration: synthesize datatree_verdict on each row.
    if version == 1:
        for row in verdicts:
            if "datatree_verdict" not in row:
                row["datatree_verdict"] = "not_attempted"  # type: ignore[typeddict-unknown-key]

    parse_tax = {k: (v[0], v[1]) for k, v in data["parse_taxonomy"].items()}
    dataset_tax = {k: (v[0], v[1]) for k, v in data["dataset_taxonomy"].items()}
    datatree_tax: dict[str, tuple[int, int]] = {
        k: (v[0], v[1]) for k, v in data.get("datatree_taxonomy", {}).items()
    }
    cube_results = {
        cid: CubabilityResult(
            verdict=CubabilityVerdict(info["verdict"]),
            reason=info["reason"],
            concat_dim=info.get("concat_dim"),
        )
        for cid, info in data["cubability_results"].items()
    }
    return LoadedSummary(
        verdicts=verdicts,
        parse_taxonomy=parse_tax,
        dataset_taxonomy=dataset_tax,
        datatree_taxonomy=datatree_tax,
        cubability_results=cube_results,
        other_parse_errors=[tuple(e) for e in data["other_parse_errors"]],  # type: ignore[misc]
        other_dataset_errors=[tuple(e) for e in data["other_dataset_errors"]],  # type: ignore[misc]
        other_datatree_errors=[tuple(e) for e in data.get("other_datatree_errors", [])],  # type: ignore[misc]
        generated_at=data["generated_at"],
        survey_tool_version=data["survey_tool_version"],
        virtualizarr_version=data.get("virtualizarr_version"),
        zarr_version=data.get("zarr_version"),
        xarray_version=data.get("xarray_version"),
        sampling_mode=data.get("sampling_mode"),
    )
