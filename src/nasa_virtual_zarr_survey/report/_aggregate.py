"""Aggregation: turn DuckDB rows into Python data structures the renderer can use.

Most of these helpers still take a DuckDB connection (they query ``results`` /
``collections`` directly), but everything they return is plain Python — lists,
dicts, dataclasses. That makes them easy to test without touching matplotlib
or stamping a Markdown file.

``three_phase_rows`` is the one fully-pure aggregator: feed it verdicts plus
cubability results and it produces ``ThreePhaseRow`` objects regardless of
DB state.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import duckdb

from nasa_virtual_zarr_survey.cubability import (
    CubabilityResult,
    CubabilityVerdict,
    check_cubability,
    fingerprint_from_json,
)
from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.processing_level import CUBE_MIN_RANK, parse_rank
from nasa_virtual_zarr_survey.report._ingest import attach_results
from nasa_virtual_zarr_survey.taxonomy import classify
from nasa_virtual_zarr_survey.types import Fingerprint, VerdictRow

if TYPE_CHECKING:
    from nasa_virtual_zarr_survey.db_session import SurveySession


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


def _read_sampling_mode(con: duckdb.DuckDBPyConnection) -> str | None:
    try:
        row = con.execute(
            "SELECT value FROM run_meta WHERE key = 'sampling_mode'"
        ).fetchone()
    except duckdb.CatalogException:
        return None
    return row[0] if row else None


def collect_run_metadata(
    con: duckdb.DuckDBPyConnection | None,
    survey_tool_version: str,
) -> RunMetadata:
    """Capture versions and sampling mode for a fresh (compute-from-DB) run."""
    return RunMetadata(
        generated_at=datetime.now(timezone.utc).isoformat(),
        survey_tool_version=survey_tool_version,
        virtualizarr_version=_package_version("virtualizarr"),
        zarr_version=_package_version("zarr"),
        xarray_version=_package_version("xarray"),
        sampling_mode=_read_sampling_mode(con) if con is not None else None,
    )


def _phase_verdicts(con: duckdb.DuckDBPyConnection, phase: str) -> dict[str, str]:
    """Return {collection_id: verdict} for the given phase ('parse' or 'dataset').

    Verdicts: 'all_pass', 'partial_pass', 'all_fail', 'not_attempted'.
    """
    success_col = f"{phase}_success"
    try:
        rows = con.execute(
            f"SELECT collection_concept_id, {success_col} FROM results"
        ).fetchall()
    except Exception:
        return {}

    per_coll: dict[str, list[bool | None]] = {}
    for cid, val in rows:
        if cid is None:
            continue
        per_coll.setdefault(cid, []).append(val)

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


def _top_buckets_from_db(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return {collection_concept_id: representative_bucket} from the Parquet log.

    Chooses the first non-null parse_error first, falling back to dataset_error.
    Collections with no failures map to an empty string.
    """
    try:
        rows = con.execute(
            """
            SELECT collection_concept_id,
                   any_value(parse_error_type)
                       FILTER (WHERE parse_error_type IS NOT NULL),
                   any_value(parse_error_message)
                       FILTER (WHERE parse_error_type IS NOT NULL),
                   any_value(dataset_error_type)
                       FILTER (WHERE dataset_error_type IS NOT NULL),
                   any_value(dataset_error_message)
                       FILTER (WHERE dataset_error_type IS NOT NULL)
            FROM results
            GROUP BY collection_concept_id
            """
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, str] = {}
    for cid, p_et, p_em, d_et, d_em in rows:
        if p_et:
            out[cid] = classify(p_et, p_em).value
        elif d_et:
            out[cid] = classify(d_et, d_em).value
    return out


def collection_verdicts(
    session_or_db: "SurveySession | Path | str",
    results_dir: Path | str,
    cache_filter_table: str | None = None,
) -> list[VerdictRow]:
    """Return one verdict row per collection in the DB.

    Accepts either a SurveySession (preferred) or a DuckDB path (legacy
    callers + tests). ``cache_filter_table`` is forwarded to
    :func:`attach_results` so callers can scope verdicts to a cached subset.
    """
    from nasa_virtual_zarr_survey.db_session import SurveySession

    if isinstance(session_or_db, SurveySession):
        con = session_or_db.con
    else:
        con = connect(session_or_db)
        init_schema(con)
    attach_results(con, Path(results_dir), cache_filter_table=cache_filter_table)

    parse_phase = _phase_verdicts(con, "parse")
    dataset_phase = _phase_verdicts(con, "dataset")
    datatree_phase = _phase_verdicts(con, "datatree")
    top_buckets = _top_buckets_from_db(con)

    if cache_filter_table:
        q = f"""
            SELECT c.concept_id, c.daac, c.format_family, c.skip_reason,
                   c.processing_level
            FROM collections c
            WHERE c.concept_id IN (
                SELECT DISTINCT g.collection_concept_id FROM granules g
                WHERE g.granule_concept_id IN (
                    SELECT granule_concept_id FROM {cache_filter_table}
                )
            )
        """
    else:
        q = """
            SELECT c.concept_id, c.daac, c.format_family, c.skip_reason,
                   c.processing_level
            FROM collections c
        """
    rows = con.execute(q).fetchall()
    out: list[VerdictRow] = []
    for concept_id, daac, family, skip, processing_level in rows:
        if skip:
            parse_verdict = "skipped"
            dataset_verdict = "skipped"
            datatree_verdict = "skipped"
        else:
            parse_verdict = parse_phase.get(concept_id, "not_attempted")
            dataset_verdict = dataset_phase.get(concept_id, "not_attempted")
            datatree_verdict = datatree_phase.get(concept_id, "not_attempted")
        out.append(
            VerdictRow(
                concept_id=concept_id,
                daac=daac,
                format_family=family,
                skip_reason=skip,
                processing_level=processing_level,
                parse_verdict=parse_verdict,
                dataset_verdict=dataset_verdict,
                datatree_verdict=datatree_verdict,
                top_bucket=top_buckets.get(concept_id, ""),
            )
        )
    return out


def taxonomy_counts(
    con: duckdb.DuckDBPyConnection, phase: str
) -> dict[str, tuple[int, int]]:
    """For 'parse' or 'dataset' phase, return {bucket: (granule_count, distinct_collection_count)}."""
    et_col = f"{phase}_error_type"
    try:
        rows = con.execute(
            f"SELECT collection_concept_id, {et_col}, {phase}_error_message "
            f"FROM results WHERE {et_col} IS NOT NULL"
        ).fetchall()
    except Exception:
        return {}
    granules: Counter[str] = Counter()
    colls: dict[str, set[str]] = {}
    for cid, et, em in rows:
        bucket = classify(et, em).value
        granules[bucket] += 1
        colls.setdefault(bucket, set()).add(cid)
    return {b: (granules[b], len(colls.get(b, set()))) for b in granules}


def _collection_fingerprints(
    con: duckdb.DuckDBPyConnection, verdicts: list[VerdictRow]
) -> dict[str, list[Fingerprint]]:
    """Return {collection_concept_id: [fingerprint_dict, ...]} for dataset all_pass collections."""
    eligible_ids = [
        v["concept_id"] for v in verdicts if v["dataset_verdict"] == "all_pass"
    ]
    if not eligible_ids:
        return {}
    placeholders = ",".join(["?"] * len(eligible_ids))
    try:
        rows = con.execute(
            f"SELECT collection_concept_id, fingerprint FROM results "
            f"WHERE dataset_success AND collection_concept_id IN ({placeholders})",
            eligible_ids,
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, list[Fingerprint]] = {}
    for cid, fp_json in rows:
        fp = fingerprint_from_json(fp_json)
        if fp is not None:
            out.setdefault(cid, []).append(fp)
    return out


def cubability_results(
    con: duckdb.DuckDBPyConnection, verdicts: list[VerdictRow]
) -> dict[str, CubabilityResult]:
    """Return {concept_id: CubabilityResult} for every collection."""
    fps_by_coll = _collection_fingerprints(con, verdicts)
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
    con: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str, int, list[str]]]:
    """Return ``(format_declared, skip_reason, count, example_short_names)`` rows
    for skipped collections, sorted descending by count then format.
    """
    try:
        rows = con.execute(
            """
            SELECT COALESCE(format_declared, '(null)') AS fmt,
                   skip_reason,
                   count(*) AS n,
                   list_sort(array_agg(short_name) FILTER (WHERE short_name IS NOT NULL)) AS short_names
            FROM collections
            WHERE skip_reason IS NOT NULL
            GROUP BY fmt, skip_reason
            ORDER BY n DESC, fmt
            """
        ).fetchall()
    except Exception:
        return []
    return [
        (str(fmt), str(reason), int(n), [str(s) for s in (names or [])[:3]])
        for fmt, reason, n, names in rows
    ]


def other_errors_for_phase(
    con: duckdb.DuckDBPyConnection, phase: str
) -> list[tuple[int, str, str]]:
    """Return top-50 (count, error_type, error_message) rows for errors classified as OTHER.

    Filters to the top 50 by count, then further filtered to Bucket.OTHER by the caller.
    """
    et_col = f"{phase}_error_type"
    em_col = f"{phase}_error_message"
    success_col = f"{phase}_success"
    try:
        rows = con.execute(
            f"SELECT {et_col}, {em_col}, count(*) c FROM results "
            f"WHERE {success_col} = FALSE AND {et_col} IS NOT NULL "
            f"GROUP BY 1,2 ORDER BY c DESC LIMIT 50"
        ).fetchall()
    except Exception:
        rows = []
    return [(int(c), str(et), str(em)) for et, em, c in rows]


# ---------------------------------------------------------------------------
# Pure aggregation: ThreePhaseRow
# ---------------------------------------------------------------------------


# Default for ``cube_results.get(...)`` lookups: a missing collection in the
# cube_results map (producer/consumer drift) shouldn't crash the report.
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
