"""Render `docs/results/history.md` from committed `summary.json` digests.

A snapshot is one re-run of the survey under a date-pinned dependency stack.
Each snapshot lands as a `*.summary.json` file under
`docs/results/history/`. This module reads them all, validates schema
consistency, and renders a Coverage-over-time page with funnel/bucket-trend
charts plus a state-transition diff and feature-introductions table.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from vzc.state._digest import LoadedSummary, load_summary

if TYPE_CHECKING:
    from vzc.render._intros import FeatureIntroduction

DEFAULT_INTROS_PATH = Path("config/feature_introductions.toml")


def _load_all(history_dir: Path) -> list[LoadedSummary]:
    """Load every `*.summary.json` under `history_dir`, sorted by snapshot_date."""
    summaries: list[LoadedSummary] = []
    for path in sorted(history_dir.glob("*.summary.json")):
        summaries.append(load_summary(path))
    # Nulls-last: dated summaries first in chronological order, then any
    # undated ones at the end. The tuple's first element puts True (None case)
    # after False so missing-date doesn't read as "prehistoric".
    summaries.sort(key=lambda s: (s.snapshot_date is None, s.snapshot_date or ""))
    return summaries


def _check_locked_sample_consistency(summaries: list[LoadedSummary]) -> str | None:
    seen: set[str] = set()
    for s in summaries:
        if s.locked_sample_sha256:
            seen.add(s.locked_sample_sha256)
    if len(seen) > 1:
        return (
            f"WARNING: locked_sample_sha256 drift across summaries: "
            f"{sorted(seen)}. Snapshots are not directly comparable."
        )
    return None


def _render_snapshot_table(summaries: list[LoadedSummary]) -> list[str]:
    lines = [
        "## Snapshots",
        "",
        "| Date | Kind | vz | xarray | zarr | locked_sample_sha256 |",
        "|---|---|---|---|---|---|",
    ]
    for s in summaries:
        lines.append(
            f"| {s.snapshot_date or '?'} | {s.snapshot_kind or '?'} | "
            f"{s.virtualizarr_version or '?'} | {s.xarray_version or '?'} | "
            f"{s.zarr_version or '?'} | "
            f"{(s.locked_sample_sha256 or '')[:12]} |"
        )
    lines.append("")
    return lines


# === Funnel-over-time chart =================================================


def _funnel_series(
    summaries: list[LoadedSummary],
) -> dict[str, list[tuple[str, float]]]:
    """For each phase, return [(snapshot_date, pct_pass)] across release snapshots."""
    series: dict[str, list[tuple[str, float]]] = {
        "parse": [],
        "dataset": [],
        "datatree": [],
        "cubability": [],
    }
    for s in summaries:
        if s.snapshot_kind != "release":
            continue
        date_ = s.snapshot_date or ""
        n_total = sum(1 for v in s.verdicts if v.get("skip_reason") is None)
        if n_total == 0:
            continue
        for phase in ("parse", "dataset", "datatree"):
            verdict_key = f"{phase}_verdict"
            n_pass = sum(1 for v in s.verdicts if v.get(verdict_key) == "all_pass")
            series[phase].append((date_, 100.0 * n_pass / n_total))
        n_feasible = sum(
            1
            for _cid, r in s.cubability_results.items()
            if r.verdict.value == "FEASIBLE"
        )
        series["cubability"].append((date_, 100.0 * n_feasible / n_total))
    return series


def _render_funnel_chart(
    series: dict[str, list[tuple[str, float]]],
    intros: list["FeatureIntroduction"],
    figures_dir: Path,
) -> tuple[Path, Path] | None:
    """Emit a holoviews+bokeh interactive HTML and a matplotlib PNG.

    Returns ``None`` when there are no release snapshots to plot.
    """
    if not any(series.values()):
        return None

    import holoviews as hv
    import matplotlib.pyplot as plt
    import pandas as pd

    figures_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for phase, points in series.items():
        for date_, pct in points:
            rows.append({"date": date_, "phase": phase, "pct": pct})
    df = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(9, 5))
    for phase in ("parse", "dataset", "datatree", "cubability"):
        sub = df[df.phase == phase].sort_values("date")
        if not sub.empty:
            ax.plot(sub.date, sub.pct, marker="o", label=phase)
    for intro in intros:
        # matplotlib accepts string x-positions when the axis is categorical
        # (snapshot dates render as strings here).
        intro_x: Any = intro.introduced.isoformat()
        ax.axvline(
            x=intro_x,
            color="gray",
            linestyle="--",
            alpha=0.5,
        )
        ax.text(
            intro_x,
            ax.get_ylim()[1] * 0.95,
            f"vz {intro.first_in_vz}: {intro.key}",
            rotation=90,
            verticalalignment="top",
            fontsize=8,
        )
    ax.set_ylabel("% of array-like collections passing")
    ax.set_xlabel("snapshot date")
    if not df.empty:
        ax.legend(loc="best")
    ax.set_title("Funnel over time")
    fig.tight_layout()
    png = figures_dir / "funnel_over_time.png"
    fig.savefig(png, dpi=110)
    plt.close(fig)

    hv.extension("bokeh")
    curves = []
    for p in ("parse", "dataset", "datatree", "cubability"):
        sub = df[df.phase == p].sort_values("date")
        if not sub.empty:
            curves.append(hv.Curve(sub, "date", "pct", label=p))
    chart = hv.Overlay(curves).opts(
        width=900, height=400, title="Funnel over time", show_grid=True
    )
    html = figures_dir / "funnel_over_time.html"
    hv.save(chart, str(html))
    return html, png


# === Bucket-trend chart ====================================================


def _bucket_trend_series(
    summaries: list[LoadedSummary], top_n: int = 10
) -> dict[str, list[tuple[str, int]]]:
    """For each top-N bucket across all phases, return [(date, count)] over snapshots."""
    totals: dict[str, int] = {}
    for s in summaries:
        if s.snapshot_kind != "release":
            continue
        for tax in (s.parse_taxonomy, s.dataset_taxonomy, s.datatree_taxonomy):
            for bucket, (n_g, _) in tax.items():
                totals[bucket] = totals.get(bucket, 0) + n_g

    top = sorted(totals.items(), key=lambda kv: -kv[1])[:top_n]
    keep = {bucket for bucket, _ in top}

    series: dict[str, list[tuple[str, int]]] = {b: [] for b in keep}
    for s in summaries:
        if s.snapshot_kind != "release":
            continue
        date_ = s.snapshot_date or ""
        per_snapshot: dict[str, int] = {}
        for tax in (s.parse_taxonomy, s.dataset_taxonomy, s.datatree_taxonomy):
            for bucket, (n_g, _) in tax.items():
                per_snapshot[bucket] = per_snapshot.get(bucket, 0) + n_g
        for b in keep:
            series[b].append((date_, per_snapshot.get(b, 0)))
    return series


def _render_bucket_trend(
    series: dict[str, list[tuple[str, int]]],
    figures_dir: Path,
) -> tuple[Path, Path] | None:
    """Emit a holoviews+bokeh interactive HTML and a matplotlib PNG.

    Returns ``None`` when there are no buckets to plot.
    """
    if not series or not any(series.values()):
        return None

    import holoviews as hv
    import matplotlib.pyplot as plt
    import pandas as pd

    figures_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for bucket, points in series.items():
        for date_, count in points:
            rows.append({"date": date_, "bucket": bucket, "count": count})
    df = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(9, 5))
    for bucket in series.keys():
        sub = df[df.bucket == bucket].sort_values("date")
        ax.plot(sub.date, sub["count"], marker="o", label=bucket)
    ax.set_ylabel("granule count")
    ax.set_title("Top failure buckets over time")
    ax.legend(loc="best", fontsize=8)
    fig.tight_layout()
    png = figures_dir / "bucket_trend.png"
    fig.savefig(png, dpi=110)
    plt.close(fig)

    hv.extension("bokeh")
    chart = hv.Overlay(
        [
            hv.Curve(df[df.bucket == b].sort_values("date"), "date", "count", label=b)
            for b in series.keys()
        ]
    ).opts(width=900, height=400, title="Top failure buckets over time", show_grid=True)
    html = figures_dir / "bucket_trend.html"
    hv.save(chart, str(html))
    return html, png


# === State-transition diff =================================================


def _render_state_transitions(summaries: list[LoadedSummary]) -> list[str]:
    releases = [s for s in summaries if s.snapshot_kind == "release"]
    if len(releases) < 2:
        return [
            "",
            "_(state-transition table requires at least 2 release snapshots)_",
            "",
        ]
    prev, curr = releases[-2], releases[-1]
    prev_by_id: dict[str, dict] = {v["concept_id"]: dict(v) for v in prev.verdicts}
    curr_by_id: dict[str, dict] = {v["concept_id"]: dict(v) for v in curr.verdicts}

    lines = [
        "",
        f"## Changes from {prev.snapshot_date} to {curr.snapshot_date}",
        "",
    ]
    for phase in ("parse", "dataset", "datatree"):
        key = f"{phase}_verdict"
        passing: list[str] = []
        failing: list[str] = []
        for cid, c_v in curr_by_id.items():
            p_v = prev_by_id.get(cid)
            if p_v is None:
                continue
            if p_v.get(key) != "all_pass" and c_v.get(key) == "all_pass":
                passing.append(cid)
            elif p_v.get(key) == "all_pass" and c_v.get(key) != "all_pass":
                failing.append(cid)
        lines.append(f"### {phase}")
        lines.append(f"- Newly passing: {', '.join(sorted(passing)) or '(none)'}")
        lines.append(f"- Newly failing: {', '.join(sorted(failing)) or '(none)'}")
        lines.append("")
    return lines


# === Preview snapshots section ============================================


def _render_preview_section(summaries: list[LoadedSummary]) -> list[str]:
    previews = [s for s in summaries if s.snapshot_kind == "preview"]
    if not previews:
        return []
    lines = ["", "## Preview snapshots", ""]
    for p in previews:
        lines.append(f"### {p.label} ({p.snapshot_date})")
        if p.description:
            lines.append("")
            lines.append(p.description)
            lines.append("")
    return lines


# === Feature introductions list ===========================================


def _render_intros_list(intros: list["FeatureIntroduction"]) -> list[str]:
    if not intros:
        return []
    lines = [
        "",
        "## Feature introductions",
        "",
        "| Date | vz | Feature | Description |",
        "|---|---|---|---|",
    ]
    for i in intros:
        lines.append(
            f"| {i.introduced.isoformat()} | {i.first_in_vz} | "
            f"`{i.key}` | {i.description} |"
        )
    lines.append("")
    return lines


# === Methodology footnote =================================================


def _render_methodology(summaries: list[LoadedSummary]) -> list[str]:
    if not summaries:
        return []
    s = summaries[-1]
    return [
        "",
        "## Methodology",
        "",
        f"- Locked sample sha256 (most recent snapshot): `{s.locked_sample_sha256 or '?'}`.",
        "- The time axis is `snapshot_date` (the `--exclude-newer DATE` value),",
        "  not the wall-clock run date.",
        "- Per-snapshot release lockfiles are sibling `.uv.lock` files; preview",
        "  snapshots reference their `config/snapshot_previews/<date>-<label>.toml`.",
        "- See the design doc at `.plans/2026-04-26-coverage-over-time-design.md`.",
        "",
    ]


# === Main entry point =====================================================


def run_history(
    history_dir: Path | str,
    out_path: Path | str,
    *,
    intros_path: Path | str = DEFAULT_INTROS_PATH,
) -> str | None:
    """Render the Coverage-over-time page from committed digests.

    Returns a warning string if locked_sample_sha256 drift was detected, else None.
    """
    from vzc.render._intros import load_introductions

    history_dir = Path(history_dir)
    out_path = Path(out_path)
    summaries = _load_all(history_dir)
    warning = _check_locked_sample_consistency(summaries)
    intros = load_introductions(intros_path)

    figures_dir = out_path.parent / "history" / "figures"

    funnel_paths = _render_funnel_chart(_funnel_series(summaries), intros, figures_dir)
    bucket_paths = _render_bucket_trend(_bucket_trend_series(summaries), figures_dir)

    lines: list[str] = [
        "# Coverage over time",
        "",
        "Time-series of VirtualiZarr coverage on cloud-hosted NASA CMR collections.",
        "",
    ]
    lines.extend(_render_snapshot_table(summaries))

    if funnel_paths is not None:
        lines.extend(
            [
                "## Funnel over time",
                "",
                '<iframe src="history/figures/funnel_over_time.html" '
                'width="100%" height="450" frameborder="0"></iframe>',
                "",
                "![Funnel over time](history/figures/funnel_over_time.png)",
                "",
            ]
        )

    if bucket_paths is not None:
        lines.extend(
            [
                "## Top failure buckets over time",
                "",
                '<iframe src="history/figures/bucket_trend.html" '
                'width="100%" height="450" frameborder="0"></iframe>',
                "",
                "![Bucket trend](history/figures/bucket_trend.png)",
                "",
            ]
        )

    lines.extend(_render_state_transitions(summaries))
    lines.extend(_render_preview_section(summaries))
    lines.extend(_render_intros_list(intros))
    lines.extend(_render_methodology(summaries))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines))
    return warning
