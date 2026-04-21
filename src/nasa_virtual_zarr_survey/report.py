"""Phase 4: roll up per-collection verdicts, apply taxonomy, render report.md."""
from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import duckdb

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.taxonomy import Bucket, classify
from nasa_virtual_zarr_survey.cubability import (
    CubabilityResult,
    CubabilityVerdict,
    check_cubability,
    fingerprint_from_json,
)


def _attach_results(con: duckdb.DuckDBPyConnection, results_dir: Path) -> bool:
    """Register a view `results` over all Parquet shards. Returns True if any exist."""
    shards = list(results_dir.glob("**/*.parquet"))
    if not shards:
        con.execute("CREATE OR REPLACE VIEW results AS SELECT * FROM (VALUES (NULL)) WHERE false")
        return False
    glob = str(results_dir / "**" / "*.parquet")
    con.execute(
        f"CREATE OR REPLACE VIEW results AS "
        f"SELECT * FROM read_parquet('{glob}', union_by_name=true, hive_partitioning=true)"
    )
    return True


def collection_verdicts(db_path: Path | str, results_dir: Path | str) -> list[dict[str, Any]]:
    """Return one verdict row per collection in the DB."""
    con = connect(db_path)
    init_schema(con)
    _attach_results(con, Path(results_dir))
    q = """
        WITH agg AS (
            SELECT collection_concept_id,
                   sum(CASE WHEN success THEN 1 ELSE 0 END) AS n_pass,
                   sum(CASE WHEN NOT success THEN 1 ELSE 0 END) AS n_fail
            FROM results
            GROUP BY collection_concept_id
        ),
        strat AS (
            SELECT collection_concept_id, MAX(stratified) AS stratified
            FROM granules
            GROUP BY collection_concept_id
        )
        SELECT c.concept_id, c.daac, c.format_family, c.skip_reason,
               COALESCE(a.n_pass, 0) AS n_pass,
               COALESCE(a.n_fail, 0) AS n_fail,
               s.stratified
        FROM collections c
        LEFT JOIN agg a ON a.collection_concept_id = c.concept_id
        LEFT JOIN strat s ON s.collection_concept_id = c.concept_id
    """
    rows = con.execute(q).fetchall()
    out = []
    for concept_id, daac, family, skip, n_pass, n_fail, stratified in rows:
        if skip:
            verdict = "skipped_format"
        elif n_pass + n_fail == 0:
            verdict = "sample_failed"
        elif n_fail == 0:
            verdict = "all_pass"
        elif n_pass == 0:
            verdict = "all_fail"
        else:
            verdict = "partial_pass"
        out.append({
            "concept_id": concept_id, "daac": daac, "format_family": family,
            "verdict": verdict, "n_pass": n_pass, "n_fail": n_fail,
            "stratified": stratified,
        })
    return out


def _taxonomy_counts(con: duckdb.DuckDBPyConnection) -> dict[str, tuple[int, int]]:
    """Per bucket, return (granule_count, distinct_collection_count)."""
    rows = con.execute(
        "SELECT collection_concept_id, error_type, error_message "
        "FROM results WHERE NOT success"
    ).fetchall()
    granules: Counter[str] = Counter()
    colls: dict[str, set[str]] = {}
    for cid, et, em in rows:
        bucket = classify(et, em).value
        granules[bucket] += 1
        colls.setdefault(bucket, set()).add(cid)
    return {b: (granules[b], len(colls.get(b, set()))) for b in granules}


def _collection_fingerprints(con: duckdb.DuckDBPyConnection, verdicts: list[dict]) -> dict[str, list[dict]]:
    """Return {collection_concept_id: [fingerprint_dict, ...]} for all_pass collections."""
    all_pass_ids = [v["concept_id"] for v in verdicts if v["verdict"] == "all_pass"]
    if not all_pass_ids:
        return {}
    placeholders = ",".join(["?"] * len(all_pass_ids))
    try:
        rows = con.execute(
            f"SELECT collection_concept_id, fingerprint FROM results "
            f"WHERE success AND collection_concept_id IN ({placeholders})",
            all_pass_ids,
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, list[dict]] = {}
    for cid, fp_json in rows:
        fp = fingerprint_from_json(fp_json)
        if fp is not None:
            out.setdefault(cid, []).append(fp)
    return out


def _cubability_results(con: duckdb.DuckDBPyConnection, verdicts: list[dict]) -> dict[str, CubabilityResult]:
    """Return {concept_id: CubabilityResult} for every collection."""
    fps_by_coll = _collection_fingerprints(con, verdicts)
    out: dict[str, CubabilityResult] = {}
    for v in verdicts:
        cid = v["concept_id"]
        if v["verdict"] != "all_pass":
            out[cid] = CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
            continue
        fps = fps_by_coll.get(cid, [])
        out[cid] = check_cubability(fps)
    return out


def render_report(verdicts: list[dict[str, Any]], tax: dict[str, tuple[int, int]],
                  con: duckdb.DuckDBPyConnection) -> str:
    total = len(verdicts)
    by_verdict = Counter(v["verdict"] for v in verdicts)
    by_daac = Counter((v["daac"], v["verdict"]) for v in verdicts)
    by_family = Counter((v["format_family"] or "SKIPPED", v["verdict"]) for v in verdicts)

    lines: list[str] = []
    lines.append("# NASA VirtualiZarr Survey Report\n")
    lines.append(f"Total collections: **{total}**\n")
    lines.append("## Verdicts\n")
    lines.append("| Verdict | Count |\n|---|---:|")
    for k, v in sorted(by_verdict.items(), key=lambda kv: -kv[1]):
        lines.append(f"| {k} | {v} |")
    lines.append("")

    lines.append("## Failure Taxonomy\n")
    lines.append("| Bucket | Granules | Collections |\n|---|---:|---:|")
    for k, (n_gran, n_coll) in sorted(tax.items(), key=lambda kv: -kv[1][0]):
        lines.append(f"| {k} | {n_gran} | {n_coll} |")
    lines.append("")

    lines.append("## By DAAC\n")
    daacs = sorted({d for d, _ in by_daac})
    verdicts_ordered = ["all_pass", "partial_pass", "all_fail", "skipped_format", "sample_failed"]
    header = "| DAAC | " + " | ".join(verdicts_ordered) + " |"
    sep = "|---|" + "|".join(["---:"] * len(verdicts_ordered)) + "|"
    lines += [header, sep]
    for d in daacs:
        cells = [str(by_daac.get((d, v), 0)) for v in verdicts_ordered]
        lines.append(f"| {d} | " + " | ".join(cells) + " |")
    lines.append("")

    lines.append("## By Format Family\n")
    fams = sorted({f for f, _ in by_family})
    header = "| Format | " + " | ".join(verdicts_ordered) + " |"
    lines += [header, sep]
    for f in fams:
        cells = [str(by_family.get((f, v), 0)) for v in verdicts_ordered]
        lines.append(f"| {f} | " + " | ".join(cells) + " |")
    lines.append("")

    lines.append("## Stratification\n")
    lines.append("| Sampling mode | Attempted | all_pass | partial_pass | all_fail |")
    lines.append("|---|---:|---:|---:|---:|")
    for mode_label, mode_filter in [
        ("stratified", lambda v: v["stratified"] is True),
        ("fallback", lambda v: v["stratified"] is False),
        ("unsampled", lambda v: v["stratified"] is None),
    ]:
        mode_vs = [v for v in verdicts if mode_filter(v)]
        attempted = len(mode_vs)
        mc = Counter(v["verdict"] for v in mode_vs)
        lines.append(
            f"| {mode_label} | {attempted} | {mc.get('all_pass', 0)} | "
            f"{mc.get('partial_pass', 0)} | {mc.get('all_fail', 0)} |"
        )
    lines.append("")

    lines.append("## Virtual Store Feasibility\n")
    comb = _cubability_results(con, verdicts)
    by_comb_verdict: Counter[str] = Counter(r.verdict.value for r in comb.values())
    lines.append("| Verdict | Count |\n|---|---:|")
    for k in ["FEASIBLE", "INCOMPATIBLE", "INCONCLUSIVE", "NOT_ATTEMPTED"]:
        if k in by_comb_verdict:
            lines.append(f"| {k} | {by_comb_verdict[k]} |")
    lines.append("")

    incompatible_reasons: Counter[str] = Counter()
    inconclusive_reasons: Counter[str] = Counter()
    examples_by_reason: dict[str, list[str]] = {}
    for cid, r in comb.items():
        if r.verdict.value == "INCOMPATIBLE":
            incompatible_reasons[r.reason] += 1
            examples_by_reason.setdefault(r.reason, []).append(cid)
        elif r.verdict.value == "INCONCLUSIVE":
            inconclusive_reasons[r.reason] += 1
            examples_by_reason.setdefault(r.reason, []).append(cid)

    if incompatible_reasons or inconclusive_reasons:
        lines.append("## Virtual Store Incompatibility Reasons\n")
        lines.append("| Verdict | Reason | Collections | Example IDs |\n|---|---|---:|---|")
        for reason, n in incompatible_reasons.most_common(10):
            ex = ", ".join(examples_by_reason[reason][:3])
            lines.append(f"| INCOMPATIBLE | {reason} | {n} | {ex} |")
        for reason, n in inconclusive_reasons.most_common(10):
            ex = ", ".join(examples_by_reason[reason][:3])
            lines.append(f"| INCONCLUSIVE | {reason} | {n} | {ex} |")
        lines.append("")

    lines.append("## Top 20 Raw Errors in `OTHER`\n")
    rows = con.execute(
        "SELECT error_type, error_message, count(*) c FROM results "
        "WHERE NOT success GROUP BY 1,2 ORDER BY c DESC LIMIT 50"
    ).fetchall()
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

    return "\n".join(lines)


def run_report(db_path: Path | str, results_dir: Path | str, out_path: Path | str) -> None:
    con = connect(db_path)
    init_schema(con)
    _attach_results(con, Path(results_dir))
    verdicts = collection_verdicts(db_path, results_dir)
    tax = _taxonomy_counts(con)
    text = render_report(verdicts, tax, con)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text)
