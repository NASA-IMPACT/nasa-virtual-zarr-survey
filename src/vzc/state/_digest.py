"""JSON digest format for survey results.

A small JSON file with everything `render_report` needs, so the docs site can
be regenerated without access to the original DuckDB / Parquet artifacts.

Schema v8 dropped the `uv_lock_sha256` and `git_overrides` fields when the
provenance machinery was simplified — version strings + locked-sample sha256
are the reproducibility contract; uv.lock copies and git-ref capture were
unnecessary detail. v7 digests fail to load and must be regenerated.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from vzc.pipeline._cubability import CubabilityResult, CubabilityVerdict
from vzc.core.types import VerdictRow

SCHEMA_VERSION = 8


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
    skipped_by_format: list[tuple[str, str, int, list[str]]] | None = None,
    survey_tool_version: str,
    virtualizarr_version: str | None = None,
    zarr_version: str | None = None,
    xarray_version: str | None = None,
    sampling_mode: str | None = None,
    generated_at: str | None = None,
    snapshot_date: str | None = None,
    snapshot_kind: str | None = None,
    label: str | None = None,
    description: str | None = None,
    locked_sample_sha256: str | None = None,
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
        "snapshot_date": snapshot_date,
        "snapshot_kind": snapshot_kind,
        "label": label,
        "description": description,
        "locked_sample_sha256": locked_sample_sha256,
        "verdicts": verdict_rows,
        "parse_taxonomy": {k: [v[0], v[1]] for k, v in parse_taxonomy.items()},
        "dataset_taxonomy": {k: [v[0], v[1]] for k, v in dataset_taxonomy.items()},
        "datatree_taxonomy": {k: [v[0], v[1]] for k, v in datatree_taxonomy.items()},
        "cubability_results": cube_serialized,
        "other_parse_errors": [list(e) for e in other_parse_errors],
        "other_dataset_errors": [list(e) for e in other_dataset_errors],
        "other_datatree_errors": [list(e) for e in other_datatree_errors],
        "skipped_by_format": [list(e) for e in (skipped_by_format or [])],
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
    skipped_by_format: list[tuple[str, str, int, list[str]]]
    generated_at: str
    survey_tool_version: str
    virtualizarr_version: str | None = None
    zarr_version: str | None = None
    xarray_version: str | None = None
    sampling_mode: str | None = None
    snapshot_date: str | None = None
    snapshot_kind: str | None = None
    label: str | None = None
    description: str | None = None
    locked_sample_sha256: str | None = None


def load_summary(path: Path | str) -> LoadedSummary:
    """Parse a summary JSON back into the typed structures used by render_report.

    Only the current ``SCHEMA_VERSION`` is accepted. Older versions raise; the
    fix is to regenerate the summary from the source DuckDB and Parquet via
    ``report --export <path>``.
    """
    data = json.loads(Path(path).read_text())
    version = data.get("schema_version")
    if version != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported schema_version: {version!r} (expected {SCHEMA_VERSION}). "
            "Regenerate via `report --export <path>` from the survey DB."
        )

    verdicts = cast(list[VerdictRow], data["verdicts"])
    parse_tax = {k: (v[0], v[1]) for k, v in data["parse_taxonomy"].items()}
    dataset_tax = {k: (v[0], v[1]) for k, v in data["dataset_taxonomy"].items()}
    datatree_tax = {k: (v[0], v[1]) for k, v in data["datatree_taxonomy"].items()}
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
        other_datatree_errors=[tuple(e) for e in data["other_datatree_errors"]],  # type: ignore[misc]
        skipped_by_format=[
            (str(fmt), str(reason), int(n), [str(s) for s in examples])
            for fmt, reason, n, examples in data["skipped_by_format"]
        ],
        generated_at=data["generated_at"],
        survey_tool_version=data["survey_tool_version"],
        virtualizarr_version=data.get("virtualizarr_version"),
        zarr_version=data.get("zarr_version"),
        xarray_version=data.get("xarray_version"),
        sampling_mode=data.get("sampling_mode"),
        snapshot_date=data.get("snapshot_date"),
        snapshot_kind=data.get("snapshot_kind"),
        label=data.get("label"),
        description=data.get("description"),
        locked_sample_sha256=data.get("locked_sample_sha256"),
    )
