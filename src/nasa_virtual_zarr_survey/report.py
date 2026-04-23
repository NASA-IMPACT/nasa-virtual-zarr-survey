"""Roll up per-collection verdicts across three phases and render report.md."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Literal
import duckdb

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.taxonomy import Bucket, classify
from nasa_virtual_zarr_survey.cubability import (
    CubabilityResult,
    CubabilityVerdict,
    check_cubability,
    fingerprint_from_json,
)
from nasa_virtual_zarr_survey.types import Fingerprint, VerdictRow


def _attach_results(con: duckdb.DuckDBPyConnection, results_dir: Path) -> bool:
    """Register a view `results` over all Parquet shards. Returns True if any exist."""
    shards = list(results_dir.glob("**/*.parquet"))
    if not shards:
        con.execute(
            "CREATE OR REPLACE VIEW results AS SELECT * FROM (VALUES (NULL)) WHERE false"
        )
        return False
    glob = str(results_dir / "**" / "*.parquet")
    con.execute(
        f"CREATE OR REPLACE VIEW results AS "
        f"SELECT * FROM read_parquet('{glob}', union_by_name=true, hive_partitioning=true)"
    )
    return True


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

    # Aggregate per collection
    per_coll: dict[str, list[bool | None]] = {}
    for cid, val in rows:
        if cid is None:
            continue
        per_coll.setdefault(cid, []).append(val)

    out: dict[str, str] = {}
    for cid, vals in per_coll.items():
        # For dataset phase, NULL means "not attempted" and doesn't count
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


def collection_verdicts(
    db_path: Path | str, results_dir: Path | str
) -> list[VerdictRow]:
    """Return one verdict row per collection in the DB."""
    con = connect(db_path)
    init_schema(con)
    _attach_results(con, Path(results_dir))

    parse_phase = _phase_verdicts(con, "parse")
    dataset_phase = _phase_verdicts(con, "dataset")

    q = """
        WITH stratification AS (
            SELECT collection_concept_id, MAX(stratified) AS stratified
            FROM granules
            GROUP BY collection_concept_id
        )
        SELECT c.concept_id, c.daac, c.format_family, c.skip_reason, s.stratified
        FROM collections c
        LEFT JOIN stratification s ON s.collection_concept_id = c.concept_id
    """
    rows = con.execute(q).fetchall()
    out: list[VerdictRow] = []
    for concept_id, daac, family, skip, stratified in rows:
        if skip:
            parse_verdict = "skipped"
            dataset_verdict = "skipped"
        else:
            parse_verdict = parse_phase.get(concept_id, "not_attempted")
            dataset_verdict = dataset_phase.get(concept_id, "not_attempted")
        out.append(
            VerdictRow(
                concept_id=concept_id,
                daac=daac,
                format_family=family,
                skip_reason=skip,
                stratified=stratified,
                parse_verdict=parse_verdict,
                dataset_verdict=dataset_verdict,
            )
        )
    return out


def _taxonomy_counts(
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


def _cubability_results(
    con: duckdb.DuckDBPyConnection, verdicts: list[VerdictRow]
) -> dict[str, CubabilityResult]:
    """Return {concept_id: CubabilityResult} for every collection."""
    fps_by_coll = _collection_fingerprints(con, verdicts)
    out: dict[str, CubabilityResult] = {}
    for v in verdicts:
        cid = v["concept_id"]
        if v["dataset_verdict"] != "all_pass":
            out[cid] = CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
            continue
        fps = fps_by_coll.get(cid, [])
        out[cid] = check_cubability(fps)
    return out


def _render_verdict_counts(
    verdicts: list[VerdictRow],
    verdict_key: Literal["parse_verdict", "dataset_verdict"],
) -> list[str]:
    by_verdict = Counter(v[verdict_key] for v in verdicts)
    lines = ["| Verdict | Count |\n|---|---:|"]
    for k, n in sorted(by_verdict.items(), key=lambda kv: -kv[1]):
        lines.append(f"| {k} | {n} |")
    return lines


def _render_taxonomy_table(tax: dict[str, tuple[int, int]], title: str) -> list[str]:
    lines = [f"### {title}\n"]
    if not tax:
        lines.append("_No failures._")
        return lines
    lines.append("| Bucket | Granules | Collections |\n|---|---:|---:|")
    for k, (n_gran, n_coll) in sorted(tax.items(), key=lambda kv: -kv[1][0]):
        lines.append(f"| {k} | {n_gran} | {n_coll} |")
    return lines


def _pct(num: int, denom: int) -> str:
    if denom == 0:
        return "n/a"
    p = round(100 * num / denom)
    return f"{num}/{denom} ({p}%)"


def _top_buckets(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
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


def _render_collections_table(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    top_buckets: dict[str, str],
) -> list[str]:
    """Render a full per-collection table near the end of the report."""
    lines = ["## Collections\n"]
    lines.append(
        "One row per sampled collection. Top bucket is the representative "
        "failure class for the collection (first parse failure if any, else "
        "first dataset failure). The Parquet log at `output/results/` has the "
        "full per-granule detail.\n"
    )
    lines.append("| concept_id | daac | format | parse | dataset | cube | top_bucket |")
    lines.append("|---|---|---|---|---|---|---|")
    for v in sorted(
        verdicts,
        key=lambda r: (r["daac"] or "", r["format_family"] or "", r["concept_id"]),
    ):
        cube = cube_results.get(
            v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
        ).verdict.value
        bucket = top_buckets.get(v["concept_id"], "")
        lines.append(
            f"| {v['concept_id']} | {v['daac'] or ''} | "
            f"{v['format_family'] or ''} | {v['parse_verdict']} | "
            f"{v['dataset_verdict']} | {cube} | {bucket or '-'} |"
        )
    lines.append("")
    return lines


def _render_three_phase_table(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    title: str,
    key: Literal["daac", "format_family"],
) -> list[str]:
    lines = [f"## {title}\n"]
    lines.append("| Group | Parsable | Datasetable | Cubable |\n|---|---|---|---|")

    groups = sorted({v[key] or "UNKNOWN" for v in verdicts})
    for group in groups:
        gv = [v for v in verdicts if (v[key] or "UNKNOWN") == group]
        total = len(gv)
        parsable = sum(1 for v in gv if v["parse_verdict"] == "all_pass")
        datasetable = sum(1 for v in gv if v["dataset_verdict"] == "all_pass")
        cubable = sum(
            1
            for v in gv
            if cube_results.get(
                v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
            ).verdict
            == CubabilityVerdict.FEASIBLE
        )
        lines.append(
            f"| {group} | {_pct(parsable, total)} | {_pct(datasetable, parsable)} | {_pct(cubable, datasetable)} |"
        )
    lines.append("")
    return lines


def render_report(
    verdicts: list[VerdictRow],
    parse_tax: dict[str, tuple[int, int]],
    dataset_tax: dict[str, tuple[int, int]],
    cube_results: dict[str, CubabilityResult],
    con: duckdb.DuckDBPyConnection,
) -> str:
    """Render the full Markdown report from pre-computed phase verdicts and taxonomy counts.

    Sections emitted, in order: totals, Phase 3 (Parsability), Phase 4
    (Datasetability), Phase 5 (Virtual Store Feasibility), incompatibility
    reasons drill-down, By DAAC table, By Format Family table, Stratification
    breakdown, and raw-error drill-downs for each phase's `OTHER` bucket.
    """
    total = len(verdicts)
    lines: list[str] = []
    lines.append("# NASA VirtualiZarr Survey Report\n")
    lines.append(f"Total collections: **{total}**\n")

    # Phase 3: Parsability
    lines.append("## Phase 3: Parsability\n")
    lines.append(
        "Per-collection verdicts based on whether the VirtualiZarr parser "
        "successfully produced a ManifestStore for each sampled granule.\n"
    )
    lines.extend(_render_verdict_counts(verdicts, "parse_verdict"))
    lines.append("")
    lines.extend(_render_taxonomy_table(parse_tax, "Parse Failure Taxonomy"))
    lines.append("")

    # Phase 4: Datasetability
    parsable_count = sum(1 for v in verdicts if v["parse_verdict"] == "all_pass")
    lines.append("## Phase 4: Datasetability\n")
    lines.append(
        f"Per-collection verdicts based on whether the ManifestStore converted to an "
        f"xarray.Dataset. Denominator: {parsable_count} collections whose sampled "
        f"granules all parsed successfully.\n"
    )
    parsable_verdicts = [v for v in verdicts if v["parse_verdict"] == "all_pass"]
    lines.extend(_render_verdict_counts(parsable_verdicts, "dataset_verdict"))
    lines.append("")
    lines.extend(_render_taxonomy_table(dataset_tax, "Dataset Failure Taxonomy"))
    lines.append("")

    # Phase 5: Virtual Store Feasibility
    datasetable_count = sum(
        1
        for v in verdicts
        if v["parse_verdict"] == "all_pass" and v["dataset_verdict"] == "all_pass"
    )
    lines.append("## Phase 5: Virtual Store Feasibility\n")
    lines.append(
        f"For collections whose all sampled granules produced xarray.Datasets "
        f"(denominator: {datasetable_count}), whether the granules can be combined "
        f"into a coherent virtual store.\n"
    )
    by_cube_verdict: Counter[str] = Counter(
        r.verdict.value for r in cube_results.values()
    )
    lines.append("| Verdict | Count |\n|---|---:|")
    for k in ["FEASIBLE", "INCOMPATIBLE", "INCONCLUSIVE", "NOT_ATTEMPTED"]:
        if k in by_cube_verdict:
            lines.append(f"| {k} | {by_cube_verdict[k]} |")
    lines.append("")

    incompatible_reasons: Counter[str] = Counter()
    inconclusive_reasons: Counter[str] = Counter()
    examples_by_reason: dict[str, list[str]] = {}
    for cid, r in cube_results.items():
        if r.verdict.value == "INCOMPATIBLE":
            incompatible_reasons[r.reason] += 1
            examples_by_reason.setdefault(r.reason, []).append(cid)
        elif r.verdict.value == "INCONCLUSIVE":
            inconclusive_reasons[r.reason] += 1
            examples_by_reason.setdefault(r.reason, []).append(cid)

    if incompatible_reasons or inconclusive_reasons:
        lines.append("### Virtual Store Incompatibility Reasons\n")
        lines.append(
            "| Verdict | Reason | Collections | Example IDs |\n|---|---|---:|---|"
        )
        for reason, n in incompatible_reasons.most_common(10):
            ex = ", ".join(examples_by_reason[reason][:3])
            lines.append(f"| INCOMPATIBLE | {reason} | {n} | {ex} |")
        for reason, n in inconclusive_reasons.most_common(10):
            ex = ", ".join(examples_by_reason[reason][:3])
            lines.append(f"| INCONCLUSIVE | {reason} | {n} | {ex} |")
        lines.append("")

    # Three-phase summary by DAAC and Format Family
    lines.extend(_render_three_phase_table(verdicts, cube_results, "By DAAC", "daac"))
    lines.extend(
        _render_three_phase_table(
            verdicts, cube_results, "By Format Family", "format_family"
        )
    )

    # Stratification
    lines.append("## Stratification\n")
    lines.append("| Sampling mode | Attempted | parse_all_pass | dataset_all_pass |")
    lines.append("|---|---:|---:|---:|")
    for mode_label, mode_filter in [
        ("stratified", lambda v: v["stratified"] is True),
        ("fallback", lambda v: v["stratified"] is False),
        ("unsampled", lambda v: v["stratified"] is None),
    ]:
        mode_vs = [v for v in verdicts if mode_filter(v)]
        attempted = len(mode_vs)
        mc_parse = Counter(v["parse_verdict"] for v in mode_vs)
        mc_dataset = Counter(v["dataset_verdict"] for v in mode_vs)
        lines.append(
            f"| {mode_label} | {attempted} | {mc_parse.get('all_pass', 0)} | "
            f"{mc_dataset.get('all_pass', 0)} |"
        )
    lines.append("")

    # Top 20 OTHER errors per phase
    for phase_label, et_col, em_col, success_col in [
        ("Parsability", "parse_error_type", "parse_error_message", "parse_success"),
        (
            "Datasetability",
            "dataset_error_type",
            "dataset_error_message",
            "dataset_success",
        ),
    ]:
        lines.append(f"## Top 20 Raw Errors in `OTHER` ({phase_label})\n")
        try:
            rows = con.execute(
                f"SELECT {et_col}, {em_col}, count(*) c FROM results "
                f"WHERE {success_col} = FALSE AND {et_col} IS NOT NULL "
                f"GROUP BY 1,2 ORDER BY c DESC LIMIT 50"
            ).fetchall()
        except Exception:
            rows = []
        shown = 0
        for et, em, c in rows:
            if classify(et, em) is Bucket.OTHER:
                lines.append(f"- **{c}x** `{et}`: {em}")
                shown += 1
                if shown >= 20:
                    break
        if shown == 0:
            lines.append("_No uncategorized errors._")
        lines.append("")

    # Full per-collection listing at the end.
    lines.extend(_render_collections_table(verdicts, cube_results, _top_buckets(con)))

    return "\n".join(lines)


def run_report(
    db_path: Path | str, results_dir: Path | str, out_path: Path | str
) -> None:
    """Read DuckDB state plus Parquet results, compute verdicts, and write `report.md`.

    Idempotent and cheap: re-run after refining `taxonomy.py` to update the
    Markdown output without re-running `attempt`.
    """
    con = connect(db_path)
    init_schema(con)
    _attach_results(con, Path(results_dir))
    verdicts = collection_verdicts(db_path, results_dir)
    parse_tax = _taxonomy_counts(con, "parse")
    dataset_tax = _taxonomy_counts(con, "dataset")
    cube_results = _cubability_results(con, verdicts)
    text = render_report(verdicts, parse_tax, dataset_tax, cube_results, con)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text)
