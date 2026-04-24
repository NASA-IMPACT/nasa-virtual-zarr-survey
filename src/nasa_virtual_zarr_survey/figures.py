"""Generate figures for the report.

Each public function writes two files: ``{stem}.png`` (matplotlib/Agg) and
``{stem}.html`` (Bokeh interactive).  Pass a Path stem without extension.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import matplotlib

# Select the non-interactive backend before anything pulls in pyplot/holoviews.
matplotlib.use("Agg")

import holoviews as hv  # noqa: E402
import holoviews.operation  # noqa: E402, F401
import matplotlib.pyplot as plt  # noqa: E402

from nasa_virtual_zarr_survey.cubability import (  # noqa: E402
    CubabilityResult,
    CubabilityVerdict,
)
from nasa_virtual_zarr_survey.types import VerdictRow  # noqa: E402

hv.extension("bokeh", "matplotlib")
hv.opts.defaults(
    hv.opts.Bars(width=800, height=450, show_grid=True),
    hv.opts.HeatMap(width=800, height=450),
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _save_html(element: hv.Element, stem: Path) -> None:
    """Save element as interactive HTML via Bokeh backend."""
    stem.parent.mkdir(parents=True, exist_ok=True)
    hv.save(element, str(stem) + ".html", backend="bokeh")


def _save_png(element: hv.Element, stem: Path) -> None:
    """Save element as PNG via matplotlib backend."""
    stem.parent.mkdir(parents=True, exist_ok=True)
    hv.save(element, str(stem) + ".png", backend="matplotlib")


def _save_both(element: hv.Element, stem: Path) -> None:
    """Save element as both interactive HTML (Bokeh) and PNG (matplotlib)."""
    _save_html(element, stem)
    _save_png(element, stem)


def _placeholder_png(stem: Path, label: str = "No data") -> None:
    """Write a minimal matplotlib placeholder PNG."""
    stem.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(4, 2))
    ax.text(
        0.5, 0.5, label, ha="center", va="center", transform=ax.transAxes, fontsize=12
    )
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(str(stem) + ".png", dpi=100)
    plt.close(fig)


def _placeholder(stem: Path, label: str = "No data") -> None:
    """Write placeholder figures for both HTML and PNG."""
    stem.parent.mkdir(parents=True, exist_ok=True)
    el = hv.Text(0, 0, label).opts(width=400, height=200)
    _save_html(el, stem)
    _placeholder_png(stem, label)


def _funnel_tiers(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
) -> list[tuple[str, int]]:
    total = len(verdicts)
    array_like = sum(1 for v in verdicts if v["skip_reason"] is None)
    parsable = sum(1 for v in verdicts if v["parse_verdict"] == "all_pass")
    datasetable = sum(
        1
        for v in verdicts
        if v["parse_verdict"] == "all_pass" and v["dataset_verdict"] == "all_pass"
    )
    cubable = sum(
        1
        for v in verdicts
        if cube_results.get(
            v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
        ).verdict
        == CubabilityVerdict.FEASIBLE
    )
    return [
        ("Total discovered", total),
        ("Array-like", array_like),
        ("Parsable", parsable),
        ("Datasetable", datasetable),
        ("Cubable", cubable),
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_sankey(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    stem: Path,
) -> None:
    """Write {stem}.png and {stem}.html with a Sankey of phase attrition.

    The interactive HTML uses hv.Sankey (Bokeh).  The PNG falls back to a
    horizontal bar chart since matplotlib has no Sankey support in HoloViews.
    """
    if not verdicts:
        _placeholder(stem, "No data for Sankey")
        return

    total = len(verdicts)
    array_like = sum(1 for v in verdicts if v["skip_reason"] is None)
    skipped = total - array_like
    parsable = sum(1 for v in verdicts if v["parse_verdict"] == "all_pass")
    parse_fail = sum(
        1 for v in verdicts if v["parse_verdict"] in ("all_fail", "partial_pass")
    )
    parse_na = array_like - parsable - parse_fail
    datasetable = sum(
        1
        for v in verdicts
        if v["parse_verdict"] == "all_pass" and v["dataset_verdict"] == "all_pass"
    )
    dataset_fail = sum(
        1
        for v in verdicts
        if v["parse_verdict"] == "all_pass"
        and v["dataset_verdict"] in ("all_fail", "partial_pass")
    )
    dataset_na = parsable - datasetable - dataset_fail
    cubable = sum(
        1
        for v in verdicts
        if cube_results.get(
            v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
        ).verdict
        == CubabilityVerdict.FEASIBLE
    )
    not_cubable = sum(
        1
        for v in verdicts
        if v["parse_verdict"] == "all_pass"
        and v["dataset_verdict"] == "all_pass"
        and cube_results.get(
            v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
        ).verdict
        != CubabilityVerdict.FEASIBLE
    )

    edges = []
    for src, tgt, val in [
        ("Discovered", "Array-like", array_like),
        ("Discovered", "Skipped", skipped),
        ("Array-like", "Parsable", parsable),
        ("Array-like", "Parse fail", parse_fail),
        ("Array-like", "Not attempted (parse)", parse_na),
        ("Parsable", "Datasetable", datasetable),
        ("Parsable", "Dataset fail", dataset_fail),
        ("Parsable", "Not attempted (dataset)", dataset_na),
        ("Datasetable", "Cubable", cubable),
        ("Datasetable", "Not cubable", not_cubable),
    ]:
        if val > 0:
            edges.append((src, tgt, val))

    if not edges:
        _placeholder(stem, "No data for Sankey")
        return

    # Interactive HTML: hv.Sankey (Bokeh only)
    sankey = hv.Sankey(edges, vdims="Count").opts(
        hv.opts.Sankey(
            width=900,
            height=500,
            label_position="left",
            title="Survey phase attrition",
        )
    )
    _save_html(sankey, stem)

    # Static PNG: funnel bars (matplotlib does not support Sankey in HoloViews)
    tiers = _funnel_tiers(verdicts, cube_results)
    labels = [label for label, _ in tiers]
    counts = [count for _, count in tiers]
    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.barh(labels, counts, color="#2196F3")
    ax.invert_yaxis()
    ax.set_xlabel("Collections")
    ax.set_title("Survey phase attrition (funnel view)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(str(stem) + ".png", dpi=150)
    plt.close(fig)


def generate_funnel(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    stem: Path,
) -> None:
    """Write {stem}.png and {stem}.html: horizontal bar funnel."""
    tiers = _funnel_tiers(verdicts, cube_results)
    if not tiers or all(v == 0 for _, v in tiers):
        _placeholder(stem, "No data for funnel")
        return

    data = [(label, count) for label, count in tiers]
    bars = hv.Bars(data, kdims=["Phase"], vdims=["Collections"]).opts(
        hv.opts.Bars(
            title="Survey funnel",
            invert_axes=True,
            width=800,
            height=350,
            color="#2196F3",
            show_legend=False,
        )
    )
    _save_both(bars, stem)


def generate_taxonomy(
    tax: dict[str, tuple[int, int]],
    title: str,
    stem: Path,
) -> None:
    """Write {stem}.png and {stem}.html: grouped bar chart of failure taxonomy."""
    if not tax:
        _placeholder(stem, "No failures")
        return

    items = sorted(tax.items(), key=lambda kv: -kv[1][0])
    data = []
    for bucket, (gran, coll) in items:
        data.append((bucket, "Granules", gran))
        data.append((bucket, "Collections", coll))

    bars = hv.Bars(data, kdims=["Bucket", "Series"], vdims=["Count"]).opts(
        hv.opts.Bars(
            title=title,
            width=800,
            height=400,
            multi_level=True,
            show_legend=True,
            xrotation=45,
        )
    )
    _save_both(bars, stem)


def generate_group_bars(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    group_key: Literal["daac", "format_family"],
    title: str,
    stem: Path,
) -> None:
    """Write {stem}.png and {stem}.html: pass-rate bars grouped by DAAC or format."""
    if not verdicts:
        _placeholder(stem, "No data")
        return

    groups = sorted({v[group_key] or "UNKNOWN" for v in verdicts})  # type: ignore[literal-required]

    def _pct(k: int, n: int) -> float:
        return 100.0 * k / n if n > 0 else 0.0

    data = []
    for group in groups:
        gv = [v for v in verdicts if (v[group_key] or "UNKNOWN") == group]  # type: ignore[literal-required]
        total = len(gv)
        parsable = sum(1 for v in gv if v["parse_verdict"] == "all_pass")
        parsable_vs = [v for v in gv if v["parse_verdict"] == "all_pass"]
        datasetable = sum(1 for v in parsable_vs if v["dataset_verdict"] == "all_pass")
        datasetable_vs = [v for v in parsable_vs if v["dataset_verdict"] == "all_pass"]
        cubable = sum(
            1
            for v in datasetable_vs
            if cube_results.get(
                v["concept_id"], CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
            ).verdict
            == CubabilityVerdict.FEASIBLE
        )
        data.append((group, "Parsable %", _pct(parsable, total)))
        data.append((group, "Datasetable %", _pct(datasetable, parsable)))
        data.append((group, "Cubable %", _pct(cubable, len(datasetable_vs))))

    bars = hv.Bars(data, kdims=["Group", "Phase"], vdims=["Pass rate (%)"]).opts(
        hv.opts.Bars(
            title=title,
            width=max(800, len(groups) * 80 + 200),
            height=450,
            multi_level=True,
            show_legend=True,
            xrotation=45,
            ylim=(0, 115),
        )
    )
    _save_both(bars, stem)


def generate_heatmap(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    stem: Path,
) -> None:
    """Write {stem}.png and {stem}.html: per-collection phase outcome heatmap.

    Outcomes are mapped to integers so HoloViews can use a numeric colormap
    across both Bokeh and matplotlib backends (avoiding ArrowStringArray
    reshape incompatibility with categorical vdims).
    """
    if not verdicts:
        _placeholder(stem, "No data")
        return

    sorted_verdicts = sorted(
        verdicts,
        key=lambda v: (v["daac"] or "", v["format_family"] or "", v["concept_id"]),
    )

    # Map outcome strings to integers; lower = better
    _VERDICT_NUM = {
        "all_pass": 4,
        "FEASIBLE": 4,
        "partial_pass": 3,
        "all_fail": 1,
        "INCOMPATIBLE": 1,
        "INCONCLUSIVE": 2,
        "skipped": 2,
        "not_attempted": 0,
        "NOT_ATTEMPTED": 0,
    }

    data = []
    for v in sorted_verdicts:
        cid = v["concept_id"]
        cube_verdict = cube_results.get(
            cid, CubabilityResult(CubabilityVerdict.NOT_ATTEMPTED)
        ).verdict.value
        for phase, raw in [
            ("Parsability", v["parse_verdict"]),
            ("Datasetability", v["dataset_verdict"]),
            ("Cubability", cube_verdict),
        ]:
            data.append((phase, cid, _VERDICT_NUM.get(raw, 0)))

    heatmap = hv.HeatMap(data, kdims=["Phase", "Collection"], vdims=["Score"]).opts(
        hv.opts.HeatMap(
            title="Per-collection phase outcomes",
            width=500,
            height=max(300, len(sorted_verdicts) * 12 + 100),
            colorbar=True,
            xrotation=0,
            cmap="RdYlGn",
            clim=(0, 4),
        )
    )
    _save_html(heatmap, stem)
    _save_png(heatmap, stem)


def generate_all(
    verdicts: list[VerdictRow],
    cube_results: dict[str, CubabilityResult],
    parse_tax: dict[str, tuple[int, int]],
    dataset_tax: dict[str, tuple[int, int]],
    out_dir: Path,
) -> dict[str, Path]:
    """Generate every figure. Returns {name: stem} (without extension)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    stems: dict[str, Path] = {
        "sankey": out_dir / "sankey",
        "funnel": out_dir / "funnel",
        "taxonomy_parse": out_dir / "taxonomy_parse",
        "taxonomy_dataset": out_dir / "taxonomy_dataset",
        "by_daac": out_dir / "by_daac",
        "by_format": out_dir / "by_format",
        "collections": out_dir / "collections",
    }
    generate_sankey(verdicts, cube_results, stems["sankey"])
    generate_funnel(verdicts, cube_results, stems["funnel"])
    generate_taxonomy(parse_tax, "Parse failure taxonomy", stems["taxonomy_parse"])
    generate_taxonomy(
        dataset_tax, "Dataset failure taxonomy", stems["taxonomy_dataset"]
    )
    generate_group_bars(
        verdicts, cube_results, "daac", "Pass rate by DAAC", stems["by_daac"]
    )
    generate_group_bars(
        verdicts,
        cube_results,
        "format_family",
        "Pass rate by format family",
        stems["by_format"],
    )
    generate_heatmap(verdicts, cube_results, stems["collections"])
    return stems
