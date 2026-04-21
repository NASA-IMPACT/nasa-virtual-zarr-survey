"""Phase 4: roll up per-collection verdicts, apply taxonomy, render report.md."""
from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import duckdb

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.taxonomy import Bucket, classify


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
        )
        SELECT c.concept_id, c.daac, c.format_family, c.skip_reason,
               COALESCE(a.n_pass, 0) AS n_pass,
               COALESCE(a.n_fail, 0) AS n_fail
        FROM collections c
        LEFT JOIN agg a ON a.collection_concept_id = c.concept_id
    """
    rows = con.execute(q).fetchall()
    out = []
    for concept_id, daac, family, skip, n_pass, n_fail in rows:
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
        })
    return out


def _taxonomy_counts(con: duckdb.DuckDBPyConnection) -> Counter[str]:
    rows = con.execute(
        "SELECT error_type, error_message FROM results WHERE NOT success"
    ).fetchall()
    c: Counter[str] = Counter()
    for et, em in rows:
        c[classify(et, em).value] += 1
    return c


def render_report(verdicts: list[dict[str, Any]], tax: Counter[str],
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
    lines.append("| Bucket | Count |\n|---|---:|")
    for k, v in sorted(tax.items(), key=lambda kv: -kv[1]):
        lines.append(f"| {k} | {v} |")
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
