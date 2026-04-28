"""Aggregation: turn :class:`SurveyState` + Parquet results into Python data
structures the renderer can use.

Most helpers take ``state`` (the canonical :class:`SurveyState`) plus a list
of result rows already loaded from Parquet. ``three_phase_rows`` is the one
fully-pure aggregator: feed it verdicts plus cubability results and it
produces :class:`ThreePhaseRow` objects regardless of state.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from vzc.pipeline._cubability import (
    CubabilityResult,
    CubabilityVerdict,
    check_cubability,
    fingerprint_from_json,
)
from vzc.core.processing_level import CUBE_MIN_RANK, parse_rank
from vzc.state._results import iter_rows
from vzc.state._io import SurveyState
from vzc.core.taxonomy import classify
from vzc.core.types import Fingerprint, VerdictRow


@dataclass(frozen=True)
class RunMetadata:
    """Versions and invocation context stamped on a survey report run."""

    generated_at: str
    survey_tool_version: str
    virtualizarr_version: str | None = None
    zarr_version: str | None = None
    xarray_version: str | None = None
    sampling_mode: str | None = None


@dataclass(frozen=True)
class ThreePhaseRow:
    """One row of the per-DAAC / per-format-family funnel table.

    Each ``(n, total)`` pair feeds a ``num/denom (pct%)`` cell. ``parsable``'s
    denominator is the group total; the dataset / datatree denominators are
    the parsable count; ``cubable``'s denominator excludes collections that
    Phase 5 marks ``EXCLUDED_BY_POLICY``.
    """

    group: str
    parsable: tuple[int, int]
    datasetable: tuple[int, int]
    datatreeable: tuple[int, int]
    cubable: tuple[int, int]


def _package_version(name: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(name)
    except PackageNotFoundError:
        return None


def collect_run_metadata(
    state: SurveyState | None,
    survey_tool_version: str,
) -> RunMetadata:
    """Capture versions and sampling mode for a fresh (compute-from-state) run."""
    return RunMetadata(
        generated_at=datetime.now(timezone.utc).isoformat(),
        survey_tool_version=survey_tool_version,
        virtualizarr_version=_package_version("virtualizarr"),
        zarr_version=_package_version("zarr"),
        xarray_version=_package_version("xarray"),
        sampling_mode=state.run_meta.get("sampling_mode") if state else None,
    )


def _phase_verdict_per_collection(
    rows: list[dict[str, Any]], phase: str
) -> dict[str, str]:
    """Return ``{collection_id: verdict}`` for the given phase ('parse' or 'dataset' or 'datatree').

    Verdicts: 'all_pass', 'partial_pass', 'all_fail', 'not_attempted'.
    """
    success_col = f"{phase}_success"
    per_coll: dict[str, list[bool | None]] = {}
    for r in rows:
        cid = r.get("collection_concept_id")
        if cid is None:
            continue
        per_coll.setdefault(cid, []).append(r.get(success_col))

    out: dict[str, str] = {}
    for cid, vals in per_coll.items():
        attempted = [v for v in vals if v is not None]
        if not attempted:
            out[cid] = "not_attempted"
        elif all(attempted):
            out[cid] = "all_pass"
        elif not any(attempted):
            out[cid] = "all_fail"
        else:
            out[cid] = "partial_pass"
    return out


def _top_buckets(rows: list[dict[str, Any]]) -> dict[str, str]:
    """Return ``{collection_concept_id: representative_bucket}`` from result rows.

    Chooses the first non-null parse_error first, falling back to dataset_error.
    Collections with no failures are absent from the map.
    """
    parse_seen: dict[str, tuple[str, str | None]] = {}
    dataset_seen: dict[str, tuple[str, str | None]] = {}
    for r in rows:
        cid = r.get("collection_concept_id")
        if cid is None:
            continue
        if cid not in parse_seen and r.get("parse_error_type"):
            parse_seen[cid] = (r["parse_error_type"], r.get("parse_error_message"))
        if cid not in dataset_seen and r.get("dataset_error_type"):
            dataset_seen[cid] = (
                r["dataset_error_type"],
                r.get("dataset_error_message"),
            )
    out: dict[str, str] = {}
    for cid in set(parse_seen) | set(dataset_seen):
        if cid in parse_seen:
            et, em = parse_seen[cid]
        else:
            et, em = dataset_seen[cid]
        out[cid] = classify(et, em).value
    return out


def collection_verdicts(
    state: SurveyState,
    results_dir: Path | str,
) -> list[VerdictRow]:
    """Return one verdict row per collection in ``state``."""
    rows = list(iter_rows(results_dir))
    parse_phase = _phase_verdict_per_collection(rows, "parse")
    dataset_phase = _phase_verdict_per_collection(rows, "dataset")
    datatree_phase = _phase_verdict_per_collection(rows, "datatree")
    top_buckets = _top_buckets(rows)

    out: list[VerdictRow] = []
    for c in state.collections:
        if c.skip_reason:
            parse_verdict = "skipped"
            dataset_verdict = "skipped"
            datatree_verdict = "skipped"
        else:
            parse_verdict = parse_phase.get(c.concept_id, "not_attempted")
            dataset_verdict = dataset_phase.get(c.concept_id, "not_attempted")
            datatree_verdict = datatree_phase.get(c.concept_id, "not_attempted")
        out.append(
            VerdictRow(
                concept_id=c.concept_id,
                daac=c.daac,
                format_family=c.format_family,
                skip_reason=c.skip_reason,
                processing_level=c.processing_level,
                parse_verdict=parse_verdict,
                dataset_verdict=dataset_verdict,
                datatree_verdict=datatree_verdict,
                top_bucket=top_buckets.get(c.concept_id, ""),
            )
        )
    return out


def taxonomy_counts(results_dir: Path | str, phase: str) -> dict[str, tuple[int, int]]:
    """``{bucket: (granule_count, distinct_collection_count)}`` for a phase."""
    et_col = f"{phase}_error_type"
    em_col = f"{phase}_error_message"
    granules: Counter[str] = Counter()
    colls: dict[str, set[str]] = {}
    for r in iter_rows(results_dir):
        et = r.get(et_col)
        if et is None:
            continue
        bucket = classify(et, r.get(em_col)).value
        granules[bucket] += 1
        cid = r.get("collection_concept_id")
        if cid is not None:
            colls.setdefault(bucket, set()).add(cid)
    return {b: (granules[b], len(colls.get(b, set()))) for b in granules}


def _collection_fingerprints(
    results_dir: Path | str, verdicts: list[VerdictRow]
) -> dict[str, list[Fingerprint]]:
    """``{collection_concept_id: [fingerprint, ...]}`` for dataset all-pass collections."""
    eligible = {v["concept_id"] for v in verdicts if v["dataset_verdict"] == "all_pass"}
    if not eligible:
        return {}
    out: dict[str, list[Fingerprint]] = {}
    for r in iter_rows(results_dir):
        cid = r.get("collection_concept_id")
        if cid not in eligible or not r.get("dataset_success"):
            continue
        fp = fingerprint_from_json(r.get("fingerprint"))
        if fp is not None:
            out.setdefault(cid, []).append(fp)
    return out


def cubability_results(
    results_dir: Path | str, verdicts: list[VerdictRow]
) -> dict[str, CubabilityResult]:
    """``{concept_id: CubabilityResult}`` for every collection."""
    fps_by_coll = _collection_fingerprints(results_dir, verdicts)
    out: dict[str, CubabilityResult] = {}
    for v in verdicts:
        cid = v["concept_id"]
        rank = parse_rank(v["processing_level"])
        if rank is not None and rank < CUBE_MIN_RANK:
            out[cid] = CubabilityResult(
                CubabilityVerdict.EXCLUDED_BY_POLICY,
                reason=f"processing_level={v['processing_level']} below L{CUBE_MIN_RANK}",
            )
            continue
        if v["dataset_verdict"] != "all_pass":
            out[cid] = CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
            continue
        fps = fps_by_coll.get(cid, [])
        out[cid] = check_cubability(fps)
    return out


def skipped_by_format(
    state: SurveyState,
) -> list[tuple[str, str, int, list[str]]]:
    """``(format_declared, skip_reason, count, example_short_names)`` rows
    for skipped collections, sorted descending by count then format."""
    grouped: dict[tuple[str, str], list[str]] = {}
    for c in state.collections:
        if c.skip_reason is None:
            continue
        key = (c.format_declared or "(null)", c.skip_reason)
        if c.short_name:
            grouped.setdefault(key, []).append(c.short_name)
        else:
            grouped.setdefault(key, [])

    out: list[tuple[str, str, int, list[str]]] = []
    for (fmt, reason), names in grouped.items():
        sorted_names = sorted(names)
        # Track count separately because grouped[key] may carry empty lists.
        count = sum(
            1
            for c in state.collections
            if c.skip_reason == reason and (c.format_declared or "(null)") == fmt
        )
        out.append((fmt, reason, count, sorted_names[:3]))
    out.sort(key=lambda x: (-x[2], x[0]))
    return out


def other_errors_for_phase(
    results_dir: Path | str, phase: str
) -> list[tuple[int, str, str]]:
    """Top-50 ``(count, error_type, error_message)`` tuples for ``phase`` failures.

    Filters to rows with ``{phase}_success == False`` and a non-null
    ``{phase}_error_type``. The OTHER-bucket filter is applied by callers.
    """
    et_col = f"{phase}_error_type"
    em_col = f"{phase}_error_message"
    success_col = f"{phase}_success"
    counts: Counter[tuple[str, str]] = Counter()
    for r in iter_rows(results_dir):
        if r.get(success_col) is not False:
            continue
        et = r.get(et_col)
        if et is None:
            continue
        em = r.get(em_col) or ""
        counts[(str(et), str(em))] += 1
    top = counts.most_common(50)
    return [(c, et, em) for (et, em), c in top]


# ---------------------------------------------------------------------------
# Pure aggregation: ThreePhaseRow
# ---------------------------------------------------------------------------


_NOT_ATTEMPTED = CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)


def three_phase_rows(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    key: Literal["daac", "format_family"],
) -> list[ThreePhaseRow]:
    """Roll verdicts up to one row per group (DAAC or format family).

    Pure: takes plain Python data and returns plain Python data. Same
    counting logic the Markdown layer used inline before — feed it the
    same inputs, get the same numbers.
    """
    groups = sorted({v[key] or "UNKNOWN" for v in verdicts})
    rows: list[ThreePhaseRow] = []
    for group in groups:
        gv = [v for v in verdicts if (v[key] or "UNKNOWN") == group]
        total = len(gv)
        parsable_vs = [v for v in gv if v["parse_verdict"] == "all_pass"]
        parsable = len(parsable_vs)
        datasetable = sum(1 for v in parsable_vs if v["dataset_verdict"] == "all_pass")
        datatreeable = sum(
            1 for v in parsable_vs if v["datatree_verdict"] == "all_pass"
        )
        cube_eligible = [
            v
            for v in parsable_vs
            if v["dataset_verdict"] == "all_pass"
            and cube_results.get(v["concept_id"], _NOT_ATTEMPTED).verdict
            != CubabilityVerdict.EXCLUDED_BY_POLICY
        ]
        cubable = sum(
            1
            for v in cube_eligible
            if cube_results.get(v["concept_id"], _NOT_ATTEMPTED).verdict
            == CubabilityVerdict.FEASIBLE
        )
        rows.append(
            ThreePhaseRow(
                group=group,
                parsable=(parsable, total),
                datasetable=(datasetable, parsable),
                datatreeable=(datatreeable, parsable),
                cubable=(cubable, len(cube_eligible)),
            )
        )
    return rows
