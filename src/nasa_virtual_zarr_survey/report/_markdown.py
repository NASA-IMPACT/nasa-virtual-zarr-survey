"""Markdown rendering: takes pre-computed aggregations and emits report text.

Every helper here is pure — give it the same inputs, get the same Markdown.
DuckDB and matplotlib live elsewhere.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Literal

from nasa_virtual_zarr_survey.cubability import CubabilityResult, CubabilityVerdict
from nasa_virtual_zarr_survey.processing_level import CUBE_MIN_RANK
from nasa_virtual_zarr_survey.report._aggregate import (
    RunMetadata,
    ThreePhaseRow,
    three_phase_rows,
)
from nasa_virtual_zarr_survey.taxonomy import Bucket, classify
from nasa_virtual_zarr_survey.types import VerdictRow

# Shared default for ``cube_results.get(..., _NOT_ATTEMPTED)`` lookups.
_NOT_ATTEMPTED = CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)


def _pct(num: int, denom: int) -> str:
    if denom == 0:
        return "n/a"
    p = round(100 * num / denom)
    return f"{num}/{denom} ({p}%)"


def _iframe(name: str) -> str:
    """Return a markdown-safe HTML iframe for an interactive figure."""
    return f'<iframe src="figures/{name}.html" width="100%" height="500" frameborder="0"></iframe>'


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
        cube = cube_results.get(v["concept_id"], _NOT_ATTEMPTED).verdict.value
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
    rows: list[ThreePhaseRow] = three_phase_rows(verdicts, cube_results, key)
    for r in rows:
        lines.append(
            f"| {r.group} | "
            f"{_pct(*r.parsable)} | "
            f"{_pct(*r.datasetable)} | "
            f"{_pct(*r.datatreeable)} | "
            f"{_pct(*r.cubable)} |"
        )
    lines.append("")
    return lines


def _render_metadata_block(
    meta: RunMetadata, verdicts: list[VerdictRow] | None = None
) -> list[str]:
    """Emit a bullet list of run metadata. Skips lines whose value is None."""
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
    """Emit the 'How to read this report' preamble."""
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
    """Render the "Skipped collections by declared format" section body."""
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
    """Render the full Markdown report from pre-computed phase verdicts and taxonomy counts."""
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

    lines.extend(_render_collections_table(verdicts, cube_results))
    if "collections" in fs:
        lines.append("")
        lines.append(_iframe("collections"))
        lines.append("")

    return "\n".join(lines)
