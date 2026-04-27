"""Roll up per-collection verdicts across three phases and render report.md."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal
import duckdb

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.taxonomy import Bucket, classify
from nasa_virtual_zarr_survey.cubability import (
    CubabilityResult,
    CubabilityVerdict,
    check_cubability,
    fingerprint_from_json,
)
from nasa_virtual_zarr_survey.processing_level import CUBE_MIN_RANK, parse_rank
from nasa_virtual_zarr_survey.types import Fingerprint, VerdictRow

if TYPE_CHECKING:
    from nasa_virtual_zarr_survey.db_session import SurveySession

# `figures` is imported lazily inside run_report so the base install (without
# the `docs` dependency group) can still import the report module.


@dataclass(frozen=True)
class RunMetadata:
    """Versions and invocation context stamped on a survey report run."""

    generated_at: str
    survey_tool_version: str
    virtualizarr_version: str | None = None
    zarr_version: str | None = None
    xarray_version: str | None = None
    sampling_mode: str | None = None


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


def _collect_run_metadata(
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
    session_or_db: "SurveySession | Path | str", results_dir: Path | str
) -> list[VerdictRow]:
    """Return one verdict row per collection in the DB.

    Accepts either a SurveySession (preferred) or a DuckDB path (legacy
    callers + tests).
    """
    from nasa_virtual_zarr_survey.db_session import SurveySession

    if isinstance(session_or_db, SurveySession):
        con = session_or_db.con
    else:
        con = connect(session_or_db)
        init_schema(con)
    _attach_results(con, Path(results_dir))

    parse_phase = _phase_verdicts(con, "parse")
    dataset_phase = _phase_verdicts(con, "dataset")
    datatree_phase = _phase_verdicts(con, "datatree")
    top_buckets = _top_buckets_from_db(con)

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


def _skipped_by_format(
    con: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str, int, list[str]]]:
    """Return ``(format_declared, skip_reason, count, example_short_names)`` rows
    for skipped collections, sorted descending by count then format.

    ``example_short_names`` is up to three alphabetically-sorted collection
    short names from the (fmt, reason) group, included so a reader can
    recognize which dataset families are masked behind ``(null)`` or rare
    declared formats.
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


def _other_errors_for_phase(
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


def _render_verdict_counts(
    verdicts: list[VerdictRow],
    verdict_key: Literal["parse_verdict", "dataset_verdict", "datatree_verdict"],
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


def _render_collections_table(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
) -> list[str]:
    """Render a full per-collection table near the end of the report."""
    lines = ["## Collections\n"]
    lines.append(
        "One row per sampled collection. Top bucket is the representative "
        "failure class for the collection (first parse failure if any, else "
        "first dataset failure). The Parquet log at `output/results/` has the "
        "full per-granule detail.\n"
    )
    lines.append(
        "| concept_id | daac | format | parse | dataset | datatree | cube | top_bucket |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    for v in sorted(
        verdicts,
        key=lambda r: (r["daac"] or "", r["format_family"] or "", r["concept_id"]),
    ):
        cube = cube_results.get(
            v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
        ).verdict.value
        bucket = v.get("top_bucket", "")
        lines.append(
            f"| {v['concept_id']} | {v['daac'] or ''} | "
            f"{v['format_family'] or ''} | {v['parse_verdict']} | "
            f"{v['dataset_verdict']} | {v['datatree_verdict']} | {cube} | {bucket or '-'} |"
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
    lines.append(
        "| Group | Parsable | Datasetable | Datatreeable | Cubable |\n|---|---|---|---|---|"
    )

    groups = sorted({v[key] or "UNKNOWN" for v in verdicts})
    for group in groups:
        gv = [v for v in verdicts if (v[key] or "UNKNOWN") == group]
        total = len(gv)
        parsable = sum(1 for v in gv if v["parse_verdict"] == "all_pass")
        parsable_vs = [v for v in gv if v["parse_verdict"] == "all_pass"]
        datasetable = sum(1 for v in parsable_vs if v["dataset_verdict"] == "all_pass")
        datatreeable = sum(
            1 for v in parsable_vs if v["datatree_verdict"] == "all_pass"
        )
        cube_eligible = [
            v
            for v in parsable_vs
            if v["dataset_verdict"] == "all_pass"
            and cube_results.get(
                v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
            ).verdict
            != CubabilityVerdict.EXCLUDED_BY_POLICY
        ]
        cubable = sum(
            1
            for v in cube_eligible
            if cube_results[v["concept_id"]].verdict == CubabilityVerdict.FEASIBLE
        )
        lines.append(
            f"| {group} | {_pct(parsable, total)} | {_pct(datasetable, parsable)} | "
            f"{_pct(datatreeable, parsable)} | {_pct(cubable, len(cube_eligible))} |"
        )
    lines.append("")
    return lines


def _iframe(name: str) -> str:
    """Return a markdown-safe HTML iframe for an interactive figure."""
    return f'<iframe src="figures/{name}.html" width="100%" height="500" frameborder="0"></iframe>'


def _render_metadata_block(
    meta: RunMetadata, verdicts: list[VerdictRow] | None = None
) -> list[str]:
    """Emit a bullet list of run metadata. Skips lines whose value is None.

    When ``verdicts`` is provided, also emits derived run-scope rows
    (collections sampled, DAACs covered, format families seen).
    """
    rows: list[tuple[str, str | None]] = [
        ("Generated", meta.generated_at),
        ("Survey tool", meta.survey_tool_version),
        ("VirtualiZarr", meta.virtualizarr_version),
        ("Zarr", meta.zarr_version),
        ("Xarray", meta.xarray_version),
        ("Sampling mode", meta.sampling_mode),
    ]
    if verdicts is not None:
        total = len(verdicts)
        skipped = sum(1 for v in verdicts if v.get("skip_reason"))
        sampled = total - skipped
        daacs = sorted({v["daac"] for v in verdicts if v["daac"]})
        families = sorted({v["format_family"] for v in verdicts if v["format_family"]})
        rows.append(
            (
                "Collections",
                f"{total} total ({sampled} sampled, {skipped} skipped pre-sample)",
            )
        )
        if daacs:
            rows.append(("DAACs covered", f"{len(daacs)} ({', '.join(daacs)})"))
        if families:
            rows.append(("Format families seen", ", ".join(families)))
    lines = [f"- **{label}:** {value}" for label, value in rows if value]
    lines.append("")
    return lines


def _render_reading_guide() -> list[str]:
    """Emit the 'How to read this report' preamble shown above every report.

    Cross-links to the docs site so the report is intelligible for a reader who
    arrived via search or a direct DAAC link.
    """
    return [
        "## How to read this report\n",
        (
            "The survey runs five phases against each cloud-hosted CMR "
            "collection: **Discover** (1), **Sample** (2), **Parsability** "
            "(3), **Datasetability / Datatreeability** (4a / 4b), and "
            "**Cubability** (5). Each phase below shows per-collection "
            "verdicts and, for failures, a [taxonomy](../design/taxonomy.md) "
            "bucket.\n"
        ),
        (
            "Verdict labels are `all_pass`, `partial_pass`, `all_fail`, "
            "`not_attempted`, and `skipped`. See the "
            "[glossary](../glossary.md) for definitions of these and other "
            "terms (granule, DAAC, ManifestStore, fingerprint, sampling). "
            "For methodology and the run-mode flags that produced "
            "this report, see the [usage docs](../index.md).\n"
        ),
        (
            "Tip: search this page (`Ctrl-F`) for your DAAC short code "
            "(e.g. `LPCLOUD`, `POCLOUD`) or a CMR concept ID to jump straight "
            "to your collection's row in the table at the bottom.\n"
        ),
    ]


def _render_skipped_by_format(
    rows: list[tuple[str, str, int, list[str]]],
) -> list[str]:
    """Render the "Skipped collections by declared format" section body.

    ``rows`` is ``(format_declared, skip_reason, count, example_short_names)``
    sorted descending. The ``Example collections`` column is omitted when no
    row carries any examples (e.g. when re-rendering from an older summary).
    """
    lines = ["## Skipped collections by declared format\n"]
    lines.append(
        "Collections filtered out before sampling because no VirtualiZarr parser "
        "exists for the declared format. Sorted descending by count; the top "
        "rows are the highest-impact targets for new-parser work. A `(null)` "
        "declared format means the CMR record didn't list a format in either "
        "`FileDistributionInformation` or `FileArchiveInformation`; the "
        "`Example collections` column shows representative short names so the "
        "underlying dataset family is still recognizable.\n"
    )
    if not rows:
        lines.append("_No skipped collections._")
        lines.append("")
        return lines
    has_examples = any(examples for *_, examples in rows)
    if has_examples:
        lines.append("| Declared format | Reason | Collections | Example collections |")
        lines.append("|---|---|---:|---|")
        for fmt, reason, n, examples in rows:
            ex = ", ".join(examples) if examples else ""
            lines.append(f"| {fmt} | {reason} | {n} | {ex} |")
    else:
        lines.append("| Declared format | Reason | Collections |")
        lines.append("|---|---|---:|")
        for fmt, reason, n, _examples in rows:
            lines.append(f"| {fmt} | {reason} | {n} |")
    lines.append("")
    return lines


def render_report(
    verdicts: list[VerdictRow],
    parse_tax: dict[str, tuple[int, int]],
    dataset_tax: dict[str, tuple[int, int]],
    cube_results: dict[str, CubabilityResult],
    other_parse_errors: list[tuple[int, str, str]],
    other_dataset_errors: list[tuple[int, str, str]],
    figure_stems: dict[str, Path] | None = None,
    datatree_tax: dict[str, tuple[int, int]] | None = None,
    other_datatree_errors: list[tuple[int, str, str]] | None = None,
    metadata: RunMetadata | None = None,
    skipped_by_format: list[tuple[str, str, int, list[str]]] | None = None,
) -> str:
    """Render the full Markdown report from pre-computed phase verdicts and taxonomy counts.

    Sections emitted, in order: Overview (Sankey), totals (with funnel figure),
    Phase 3 (Parsability, with parse taxonomy figure), Phase 4a (Datasetability,
    with dataset taxonomy figure), Phase 4b (Datatreeability, with datatree
    taxonomy figure), Phase 5 (Cubability),
    incompatibility reasons drill-down, By DAAC table (with by_daac figure),
    By Format Family table (with by_format figure), Stratification breakdown,
    raw-error drill-downs for each phase's ``OTHER`` bucket, and the full
    per-collection table (with collections heatmap figure).

    If ``figure_stems`` is provided (mapping name to stem Path without extension),
    interactive HTML figures are embedded via ``<iframe>`` elements.  The PNG
    files also live under ``figures/`` for reference.  The caller is responsible
    for generating the figures before calling this function.

    ``other_parse_errors``, ``other_dataset_errors``, and ``other_datatree_errors``
    are lists of ``(count, error_type, error_message)`` triples (top 50 by count,
    pre-computed by the caller). Only entries classified as Bucket.OTHER are rendered.
    """
    fs = figure_stems or {}
    _datatree_tax: dict[str, tuple[int, int]] = datatree_tax or {}
    _other_datatree_errors: list[tuple[int, str, str]] = other_datatree_errors or []
    _skipped_by_format: list[tuple[str, str, int, list[str]]] = skipped_by_format or []

    total = len(verdicts)
    lines: list[str] = []
    lines.append("# NASA VirtualiZarr Survey Report\n")
    lines.append("Historical snapshots: see [Coverage over time](history.md).\n")

    if metadata is not None:
        lines.extend(_render_metadata_block(metadata, verdicts))

    lines.extend(_render_reading_guide())

    # Overview section with Sankey
    lines.append("## Overview\n")
    if "sankey" in fs:
        lines.append(_iframe("sankey"))
        lines.append("")

    lines.append(f"Total collections: **{total}**\n")
    if "funnel" in fs:
        lines.append(_iframe("funnel"))
        lines.append("")

    lines.extend(_render_skipped_by_format(_skipped_by_format))

    # Phase 3: Parsability
    lines.append("## Phase 3: Parsability\n")
    lines.append(
        "Per-collection verdicts based on whether the VirtualiZarr parser "
        "successfully produced a ManifestStore for each sampled granule. "
        "Failure-bucket meanings: see "
        "[the taxonomy reference](../design/taxonomy.md).\n"
    )
    lines.extend(_render_verdict_counts(verdicts, "parse_verdict"))
    lines.append("")
    lines.extend(_render_taxonomy_table(parse_tax, "Parse Failure Taxonomy"))
    lines.append("")
    if parse_tax and "taxonomy_parse" in fs:
        lines.append(_iframe("taxonomy_parse"))
        lines.append("")

    # Phase 4a: Datasetability
    parsable_count = sum(1 for v in verdicts if v["parse_verdict"] == "all_pass")
    lines.append("## Phase 4a: Datasetability\n")
    lines.append(
        f"Per-collection verdicts based on whether the ManifestStore converted to an "
        f"xarray.Dataset. Denominator: {parsable_count} collections whose sampled "
        f"granules all parsed successfully. Failure-bucket meanings: see "
        f"[the taxonomy reference](../design/taxonomy.md).\n"
    )
    parsable_verdicts = [v for v in verdicts if v["parse_verdict"] == "all_pass"]
    lines.extend(_render_verdict_counts(parsable_verdicts, "dataset_verdict"))
    lines.append("")
    lines.extend(_render_taxonomy_table(dataset_tax, "Dataset Failure Taxonomy"))
    lines.append("")
    if dataset_tax and "taxonomy_dataset" in fs:
        lines.append(_iframe("taxonomy_dataset"))
        lines.append("")

    # Phase 4b: Datatreeability
    lines.append("## Phase 4b: Datatreeability\n")
    lines.append(
        f"Per-collection verdicts based on whether the ManifestStore converted to an "
        f"xarray.DataTree. Attempted in parallel with Phase 4a for all collections "
        f"that parsed successfully (denominator: {parsable_count}). "
        f"Failure-bucket meanings: see "
        f"[the taxonomy reference](../design/taxonomy.md).\n"
    )
    rescued_by_datatree = sum(
        1
        for v in parsable_verdicts
        if v["dataset_verdict"] != "all_pass"
        and v["top_bucket"] == Bucket.CONFLICTING_DIM_SIZES.value
        and v["datatree_verdict"] == "all_pass"
    )
    lines.append(
        f"**Rescued by Phase 4b:** {rescued_by_datatree} collection(s) that failed "
        f"Phase 4a (`CONFLICTING_DIM_SIZES`) succeeded under Phase 4b.\n"
    )
    lines.extend(_render_verdict_counts(parsable_verdicts, "datatree_verdict"))
    lines.append("")
    lines.extend(_render_taxonomy_table(_datatree_tax, "Datatree Failure Taxonomy"))
    lines.append("")
    if _datatree_tax and "taxonomy_datatree" in fs:
        lines.append(_iframe("taxonomy_datatree"))
        lines.append("")

    # Phase 5: Cubability
    datasetable_count = sum(
        1
        for v in verdicts
        if v["parse_verdict"] == "all_pass" and v["dataset_verdict"] == "all_pass"
    )
    excluded_count = sum(
        1
        for r in cube_results.values()
        if r.verdict == CubabilityVerdict.EXCLUDED_BY_POLICY
    )
    lines.append("## Phase 5: Cubability\n")
    lines.append(
        f"For collections whose all sampled granules produced xarray.Datasets "
        f"(denominator: {datasetable_count}), whether the granules can be combined "
        f"into a coherent virtual store. {excluded_count} collection(s) below L"
        f"{CUBE_MIN_RANK} are excluded by policy as inherently non-gridded.\n"
    )
    by_cube_verdict: Counter[str] = Counter(
        r.verdict.value for r in cube_results.values()
    )
    lines.append("| Verdict | Count |\n|---|---:|")
    for k in [
        "FEASIBLE",
        "INCOMPATIBLE",
        "INCONCLUSIVE",
        "NOT_ATTEMPTED",
        "EXCLUDED_BY_POLICY",
    ]:
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
        lines.append("### Cubability Incompatibility Reasons\n")
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
    if "by_daac" in fs:
        lines.append("## By DAAC\n")
        lines.append(_iframe("by_daac"))
        lines.append("")
        lines.extend(
            _render_three_phase_table(verdicts, cube_results, "By DAAC", "daac")[1:]
        )
    else:
        lines.extend(
            _render_three_phase_table(verdicts, cube_results, "By DAAC", "daac")
        )

    if "by_format" in fs:
        lines.append("## By Format Family\n")
        lines.append(_iframe("by_format"))
        lines.append("")
        lines.extend(
            _render_three_phase_table(
                verdicts, cube_results, "By Format Family", "format_family"
            )[1:]
        )
    else:
        lines.extend(
            _render_three_phase_table(
                verdicts, cube_results, "By Format Family", "format_family"
            )
        )

    # Top 20 OTHER errors per phase (pre-computed by caller)
    for phase_label, error_list in [
        ("Parsability", other_parse_errors),
        ("Datasetability", other_dataset_errors),
        ("Datatreeability", _other_datatree_errors),
    ]:
        lines.append(f"## Top 20 Raw Errors in `OTHER` ({phase_label})\n")
        shown = 0
        for c, et, em in error_list:
            if classify(et, em) is Bucket.OTHER:
                lines.append(f"- **{c}x** `{et}`: {em}")
                shown += 1
                if shown >= 20:
                    break
        if shown == 0:
            lines.append("_No uncategorized errors._")
        lines.append("")

    # Full per-collection listing at the end.
    lines.extend(_render_collections_table(verdicts, cube_results))
    if "collections" in fs:
        lines.append("")
        lines.append(_iframe("collections"))
        lines.append("")

    return "\n".join(lines)


def _sha256_of_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_report(
    session: "SurveySession | Path | str | None",
    results_dir: Path | str,
    out_path: Path | str = "docs/results/index.md",
    *,
    export_to: Path | str | None = None,
    from_data: Path | str | None = None,
    snapshot_date: str | None = None,
    snapshot_kind: str | None = None,
    label: str | None = None,
    description: str | None = None,
    git_overrides: dict[str, dict[str, str]] | None = None,
    locked_sample_path: Path | str | None = None,
    uv_lock_path: Path | str | None = None,
    preview_manifest_path: Path | str | None = None,
    no_render: bool = False,
) -> None:
    """Read DuckDB state plus Parquet results, compute verdicts, and write the report.

    Idempotent and cheap: re-run after refining ``taxonomy.py`` to update the
    Markdown output without re-running ``attempt``.

    Parameters
    ----------
    session:
        A :class:`SurveySession` (or, for backwards compatibility with older
        callers/tests, a DuckDB path). ``None`` is required when
        ``from_data`` is set.
    results_dir:
        Directory containing Parquet result shards (ignored when
        ``from_data`` is set).
    out_path:
        Destination Markdown file for the rendered report.
    export_to:
        When provided, serialize all computed data to a compact JSON digest at
        this path after computing verdicts. Mutually exclusive with
        ``from_data``.
    from_data:
        When provided, load verdicts and taxonomy from the given JSON digest
        and skip DuckDB/Parquet queries entirely.
    snapshot_date:
        ISO date for the snapshot (e.g., ``2026-02-15``). When set without
        ``preview_manifest_path``, the exported summary is tagged
        ``snapshot_kind="release"``.
    locked_sample_path:
        Path to the locked-sample JSON sourcing the session. Hashed into
        ``locked_sample_sha256`` in the exported summary.
    uv_lock_path:
        Path to the snapshot's ``uv.lock`` (release snapshots only). Hashed
        into ``uv_lock_sha256``. Mutually exclusive with
        ``preview_manifest_path``.
    preview_manifest_path:
        Path to a ``config/snapshot_previews/*.toml`` manifest. When set, the
        exported summary is tagged ``snapshot_kind="preview"`` and label /
        description / git_overrides are read from the manifest.
    no_render:
        Skip writing the Markdown + figures output.
    """
    from nasa_virtual_zarr_survey import figures as _figures
    from nasa_virtual_zarr_survey.db_session import SurveySession

    if export_to is not None and from_data is not None:
        raise ValueError("export_to and from_data are mutually exclusive")
    if uv_lock_path is not None and preview_manifest_path is not None:
        raise ValueError(
            "uv_lock_path and preview_manifest_path are mutually exclusive"
        )

    out_path = Path(out_path)
    if not no_render:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    from nasa_virtual_zarr_survey import __version__

    effective_snapshot_date = snapshot_date

    if preview_manifest_path is not None:
        from nasa_virtual_zarr_survey.preview_manifest import load_manifest

        m = load_manifest(preview_manifest_path)
        snapshot_kind = "preview"
        label = m.label
        description = m.description or None
        git_overrides = m.git_overrides
        effective_snapshot_date = m.snapshot_date
    elif snapshot_kind is None and snapshot_date is not None:
        snapshot_kind = "release"

    if from_data is not None:
        # Regenerate report purely from committed JSON digest.
        from nasa_virtual_zarr_survey.summary_io import load_summary

        summary = load_summary(from_data)
        verdicts = summary.verdicts
        parse_tax = summary.parse_taxonomy
        dataset_tax = summary.dataset_taxonomy
        datatree_tax = summary.datatree_taxonomy
        cube_results = summary.cubability_results
        other_parse_errors = summary.other_parse_errors
        other_dataset_errors = summary.other_dataset_errors
        other_datatree_errors = summary.other_datatree_errors
        skipped_by_format = summary.skipped_by_format
        metadata = RunMetadata(
            generated_at=summary.generated_at,
            survey_tool_version=summary.survey_tool_version,
            virtualizarr_version=summary.virtualizarr_version,
            zarr_version=summary.zarr_version,
            xarray_version=summary.xarray_version,
            sampling_mode=summary.sampling_mode,
        )
    else:
        # Compute from DuckDB + Parquet via the SurveySession's connection.
        if session is None:
            raise ValueError("session is required when from_data is not set")
        if isinstance(session, SurveySession):
            con = session.con
        else:
            con = connect(session)
            init_schema(con)
        _attach_results(con, Path(results_dir))
        verdicts = collection_verdicts(session, results_dir)
        parse_tax = _taxonomy_counts(con, "parse")
        dataset_tax = _taxonomy_counts(con, "dataset")
        datatree_tax = _taxonomy_counts(con, "datatree")
        cube_results = _cubability_results(con, verdicts)
        other_parse_errors = _other_errors_for_phase(con, "parse")
        other_dataset_errors = _other_errors_for_phase(con, "dataset")
        other_datatree_errors = _other_errors_for_phase(con, "datatree")
        skipped_by_format = _skipped_by_format(con)
        metadata = _collect_run_metadata(con, __version__)

        if export_to is not None:
            from nasa_virtual_zarr_survey.summary_io import dump_summary

            locked_sha = (
                _sha256_of_file(Path(locked_sample_path))
                if locked_sample_path is not None
                else None
            )
            uv_lock_sha = (
                _sha256_of_file(Path(uv_lock_path))
                if uv_lock_path is not None
                else None
            )
            dump_summary(
                export_to,
                verdicts=verdicts,
                parse_taxonomy=parse_tax,
                dataset_taxonomy=dataset_tax,
                datatree_taxonomy=datatree_tax,
                cubability_results=cube_results,
                other_parse_errors=other_parse_errors,
                other_dataset_errors=other_dataset_errors,
                other_datatree_errors=other_datatree_errors,
                skipped_by_format=skipped_by_format,
                survey_tool_version=metadata.survey_tool_version,
                virtualizarr_version=metadata.virtualizarr_version,
                zarr_version=metadata.zarr_version,
                xarray_version=metadata.xarray_version,
                sampling_mode=metadata.sampling_mode,
                generated_at=metadata.generated_at,
                snapshot_date=effective_snapshot_date,
                snapshot_kind=snapshot_kind,
                label=label,
                description=description,
                git_overrides=git_overrides,
                locked_sample_sha256=locked_sha,
                uv_lock_sha256=uv_lock_sha,
            )

    if no_render:
        return

    figure_stems = _figures.generate_all(
        verdicts=verdicts,
        cube_results=cube_results,
        parse_tax=parse_tax,
        dataset_tax=dataset_tax,
        datatree_tax=datatree_tax,
        out_dir=out_path.parent / "figures",
    )
    text = render_report(
        verdicts,
        parse_tax,
        dataset_tax,
        cube_results,
        other_parse_errors,
        other_dataset_errors,
        figure_stems,
        datatree_tax=datatree_tax,
        other_datatree_errors=other_datatree_errors,
        metadata=metadata,
        skipped_by_format=skipped_by_format,
    )
    out_path.write_text(text)
