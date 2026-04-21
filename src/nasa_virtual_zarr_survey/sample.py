"""Phase 2: for each collection, pick N granules stratified across temporal extent."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import earthaccess

from nasa_virtual_zarr_survey.db import connect, init_schema


def temporal_bins(
    start: datetime | None, end: datetime | None, n: int
) -> list[tuple[datetime, datetime]] | None:
    """Split [start, end] into `n` equal half-open bins. None if extent missing."""
    if start is None or end is None or start >= end:
        return None
    span: timedelta = (end - start) / n
    edges = [start + i * span for i in range(n + 1)]
    edges[-1] = end
    return list(zip(edges[:-1], edges[1:]))


def _extract_url(g: Any) -> str | None:
    for link in g.data_links(access="direct") or []:
        return link
    for link in g.data_links() or []:
        return link
    return None


def _extract_size(g: Any) -> int | None:
    try:
        info = g["umm"]["DataGranule"]["ArchiveAndDistributionInformation"]
        for i in info:
            s = i.get("SizeInBytes") or i.get("Size")
            if s:
                return int(s)
    except (KeyError, TypeError, ValueError):
        pass
    return None


def sample_one_collection(coll: dict[str, Any], n_bins: int = 5) -> list[dict[str, Any]]:
    """Return up to `n_bins` granule rows for a collection, stratified over temporal bins.

    If temporal extent is missing, fall back to `n_bins` evenly-spaced offsets.
    """
    bins = temporal_bins(coll.get("time_start"), coll.get("time_end"), n=n_bins)
    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    if bins is None:
        # offset-based fallback
        num = int(coll.get("num_granules") or 0)
        step = max(1, num // n_bins) if num else 1
        for i in range(n_bins):
            results = earthaccess.search_data(
                concept_id=coll["concept_id"],
                offset=i * step,
                count=1,
            )
            if not results:
                continue
            g = results[0]
            rows.append({
                "collection_concept_id": coll["concept_id"],
                "granule_concept_id": g["meta"]["concept-id"],
                "data_url": _extract_url(g),
                "temporal_bin": i,
                "size_bytes": _extract_size(g),
                "sampled_at": now,
            })
        return rows

    for i, (a, b) in enumerate(bins):
        results = earthaccess.search_data(
            concept_id=coll["concept_id"],
            temporal=(a.isoformat(), b.isoformat()),
            count=1,
        )
        if not results:
            continue
        g = results[0]
        rows.append({
            "collection_concept_id": coll["concept_id"],
            "granule_concept_id": g["meta"]["concept-id"],
            "data_url": _extract_url(g),
            "temporal_bin": i,
            "size_bytes": _extract_size(g),
            "sampled_at": now,
        })
    return rows


def run_sample(
    db_path: Path | str, n_bins: int = 5, only_daac: str | None = None
) -> int:
    """Sample granules for every pending collection. Returns total granules written."""
    con = connect(db_path)
    init_schema(con)
    q = """
        SELECT concept_id, time_start, time_end, num_granules, daac
        FROM collections
        WHERE skip_reason IS NULL
          AND concept_id NOT IN (SELECT DISTINCT collection_concept_id FROM granules)
    """
    params: list[Any] = []
    if only_daac:
        q += " AND daac = ?"
        params.append(only_daac)
    colls = [
        {"concept_id": r[0], "time_start": r[1], "time_end": r[2],
         "num_granules": r[3], "daac": r[4]}
        for r in con.execute(q, params).fetchall()
    ]

    total = 0
    for coll in colls:
        rows = sample_one_collection(coll, n_bins=n_bins)
        for r in rows:
            con.execute(
                """INSERT OR IGNORE INTO granules VALUES (?, ?, ?, ?, ?, ?)""",
                [r["collection_concept_id"], r["granule_concept_id"], r["data_url"],
                 r["temporal_bin"], r["size_bytes"], r["sampled_at"]],
            )
            total += 1
    return total
