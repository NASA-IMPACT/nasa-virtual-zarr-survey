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

SCHEMA_VERSION = 1


def dump_summary(
    path: Path | str,
    *,
    verdicts: list[VerdictRow],
    parse_taxonomy: dict[str, tuple[int, int]],
    dataset_taxonomy: dict[str, tuple[int, int]],
    cubability_results: dict[str, CubabilityResult],
    other_parse_errors: list[tuple[int, str, str]],
    other_dataset_errors: list[tuple[int, str, str]],
    survey_tool_version: str,
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
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "survey_tool_version": survey_tool_version,
        "verdicts": verdict_rows,
        "parse_taxonomy": {k: [v[0], v[1]] for k, v in parse_taxonomy.items()},
        "dataset_taxonomy": {k: [v[0], v[1]] for k, v in dataset_taxonomy.items()},
        "cubability_results": cube_serialized,
        "other_parse_errors": [list(e) for e in other_parse_errors],
        "other_dataset_errors": [list(e) for e in other_dataset_errors],
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
    cubability_results: dict[str, CubabilityResult]
    other_parse_errors: list[tuple[int, str, str]]
    other_dataset_errors: list[tuple[int, str, str]]
    generated_at: str
    survey_tool_version: str


def load_summary(path: Path | str) -> LoadedSummary:
    """Parse a summary JSON back into the typed structures used by render_report."""
    data = json.loads(Path(path).read_text())
    if data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported schema_version: {data.get('schema_version')!r} "
            f"(expected {SCHEMA_VERSION})"
        )

    verdicts = cast(list[VerdictRow], data["verdicts"])
    parse_tax = {k: (v[0], v[1]) for k, v in data["parse_taxonomy"].items()}
    dataset_tax = {k: (v[0], v[1]) for k, v in data["dataset_taxonomy"].items()}
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
        cubability_results=cube_results,
        other_parse_errors=[tuple(e) for e in data["other_parse_errors"]],  # type: ignore[misc]
        other_dataset_errors=[tuple(e) for e in data["other_dataset_errors"]],  # type: ignore[misc]
        generated_at=data["generated_at"],
        survey_tool_version=data["survey_tool_version"],
    )
