"""Fixed-width tabular listing for ``discover --list ...``.

Pure rendering: takes plain row-mappings and an optional score map, returns
a multi-line string. Sorting follows the rank in top-N modes and
``(daac, short_name)`` otherwise.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Literal, Mapping, Sequence


def _skipped_format_breakdown(rows: Sequence[Mapping[str, Any]]) -> str:
    """Aggregate ``(format_declared, skip_reason)`` counts for skipped rows."""
    counts: Counter = Counter(
        ((r.get("format_declared") or "(null)"), r["skip_reason"])
        for r in rows
        if r.get("skip_reason")
    )
    if not counts:
        return "Skipped collections: none."
    lines = ["Skipped collections by format:"]
    for (fmt, reason), n in counts.most_common():
        lines.append(f"  {n:4d}  {fmt}  ({reason})")
    return "\n".join(lines)


def _render_collection_listing(
    rows: Sequence[Mapping[str, Any]],
    *,
    list_mode: Literal["skipped", "array", "all"],
    score_map: dict[str, tuple[int, int | None]] | None,
) -> str:
    """Render a fixed-width table of collections per ``--list <mode>``."""
    if list_mode == "skipped":
        filtered = [r for r in rows if r.get("skip_reason")]
    elif list_mode == "array":
        filtered = [r for r in rows if not r.get("skip_reason")]
    else:
        filtered = list(rows)

    if score_map is not None:

        def _sort_key(r: Mapping[str, Any]):
            rs = score_map.get(r.get("concept_id") or "")
            return (rs[0] if rs else 1_000_000_000, r.get("concept_id") or "")

        filtered.sort(key=_sort_key)
    else:
        filtered.sort(
            key=lambda r: ((r.get("daac") or ""), (r.get("short_name") or ""))
        )

    headers = [
        "rank",
        "usage_score",
        "concept_id",
        "daac",
        "fmt_family",
        "fmt_declared",
        "opendap",
        "proc_lvl",
        "short_name v version",
        "skip_reason",
        "url",
    ]
    table_rows: list[list[str]] = []
    for r in filtered:
        cid = r.get("concept_id") or ""
        rs = score_map.get(cid) if score_map else None
        rank = str(rs[0]) if rs else ""
        score = str(rs[1]) if rs and rs[1] is not None else ""
        sn = r.get("short_name") or ""
        ver = r.get("version") or ""
        sn_ver = f"{sn} v{ver}" if sn or ver else ""
        url = f"https://search.earthdata.nasa.gov/search?q={cid}" if cid else ""
        table_rows.append(
            [
                rank,
                score,
                cid,
                r.get("daac") or "",
                r.get("format_family") or "—",
                r.get("format_declared") or "(null)",
                "Y" if r.get("has_cloud_opendap") else "",
                r.get("processing_level") or "",
                sn_ver,
                r.get("skip_reason") or "",
                url,
            ]
        )

    widths = [len(h) for h in headers]
    for row in table_rows:
        for i, cell in enumerate(row):
            if len(cell) > widths[i]:
                widths[i] = len(cell)

    NUMERIC_COLS = {0, 1}

    def _fmt(cells: list[str]) -> str:
        parts = [
            cell.rjust(w) if i in NUMERIC_COLS else cell.ljust(w)
            for i, (cell, w) in enumerate(zip(cells, widths))
        ]
        return "  ".join(parts).rstrip()

    lines = [_fmt(headers)]
    for row in table_rows:
        lines.append(_fmt(row))
    return "\n".join(lines)
