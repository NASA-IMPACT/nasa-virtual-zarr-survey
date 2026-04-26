"""Phase 2 (sample): for each collection, pick N granules stratified across temporal extent."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import earthaccess

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.types import GranuleInfo, SampleCollection

if TYPE_CHECKING:
    from earthaccess.results import DataGranule


_LOGGER = logging.getLogger(__name__)


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


def _extract_url(g: DataGranule, access: str = "direct") -> str | None:
    for link in g.data_links(access=access) or []:
        return link
    return None


def _extract_urls(g: DataGranule, access: str) -> tuple[str | None, str | None]:
    """Return ``(data_url, https_url)`` for the granule.

    ``data_url`` is the URL for the requested ``access`` mode (the one the
    survey actually opens). ``https_url`` is always pulled with
    ``access="external"`` so downstream tooling (notably generated repro
    scripts) has a curl-able download URL even when the survey ran under
    ``--access direct``. When ``access`` is already ``"external"``, the two
    are the same URL and only one ``data_links`` call is made.
    """
    data_url = _extract_url(g, access=access)
    if access == "external":
        return data_url, data_url
    return data_url, _extract_url(g, access="external")


def _granule_format(g: DataGranule) -> str | None:
    """Extract a file format string from granule UMM-JSON, or None."""
    try:
        info = g["umm"]["DataGranule"]["ArchiveAndDistributionInformation"]
    except (KeyError, TypeError):
        return None
    if isinstance(info, list):
        for entry in info:
            if isinstance(entry, dict):
                fmt = entry.get("Format")
                if fmt:
                    return fmt
    elif isinstance(info, dict):
        return info.get("Format")
    return None


def _update_collection_classification(
    con, concept_id: str, format_declared: str | None
) -> str | None:
    """Re-classify a collection using a freshly-discovered format string.

    Returns the resolved skip_reason (None if array-like, else a string).
    """
    from nasa_virtual_zarr_survey.formats import classify_format

    family = classify_format(format_declared, None)
    if family is not None:
        skip_reason = None
        family_str = family.value
    elif format_declared is None:
        skip_reason = "format_unknown"
        family_str = None
    else:
        skip_reason = "non_array_format"
        family_str = None
    con.execute(
        """
        UPDATE collections
        SET format_family = ?, format_declared = ?, skip_reason = ?
        WHERE concept_id = ?
        """,
        [family_str, format_declared, skip_reason, concept_id],
    )
    return skip_reason


def _granule_dict(g: DataGranule) -> dict[str, Any]:
    """Return the full UMM-JSON dict (`{meta, umm}`) for a granule.

    Mirrors the pattern in ``discover.fetch_collection_dicts``: prefer
    ``render_dict`` when the earthaccess wrapper exposes it, otherwise
    fall back to the mapping interface.
    """
    rd = getattr(g, "render_dict", None)
    if isinstance(rd, dict):
        return rd
    return {"meta": g["meta"], "umm": g["umm"]}


def _extract_size(g: DataGranule) -> int | None:
    try:
        info = g["umm"]["DataGranule"]["ArchiveAndDistributionInformation"]
        for i in info:
            s = i.get("SizeInBytes") or i.get("Size")
            if s:
                return int(s)
    except (KeyError, TypeError, ValueError):
        pass
    return None


def sample_one_collection(
    coll: SampleCollection, n_bins: int = 5, *, access: str = "direct"
) -> list[GranuleInfo]:
    """Return up to `n_bins` granule rows for a collection, stratified over temporal bins.

    If temporal extent is missing, fall back to `n_bins` evenly-spaced offsets.
    """
    bins = temporal_bins(coll.get("time_start"), coll.get("time_end"), n=n_bins)
    rows: list[GranuleInfo] = []
    now = datetime.now(timezone.utc)

    if bins is None:
        # No temporal extent: take the first `n_bins` granules with synthetic bin indices.
        results = earthaccess.search_data(
            concept_id=coll["concept_id"],
            count=n_bins,
        )
        for i, g in enumerate(results[:n_bins]):
            data_url, https_url = _extract_urls(g, access)
            rows.append(
                GranuleInfo(
                    collection_concept_id=coll["concept_id"],
                    granule_concept_id=g["meta"]["concept-id"],
                    data_url=data_url,
                    https_url=https_url,
                    temporal_bin=i,
                    size_bytes=_extract_size(g),
                    sampled_at=now,
                    stratified=False,
                    access_mode=access,
                    umm_json=_granule_dict(g),
                )
            )
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
        data_url, https_url = _extract_urls(g, access)
        rows.append(
            GranuleInfo(
                collection_concept_id=coll["concept_id"],
                granule_concept_id=g["meta"]["concept-id"],
                data_url=data_url,
                https_url=https_url,
                temporal_bin=i,
                size_bytes=_extract_size(g),
                sampled_at=now,
                stratified=True,
                access_mode=access,
                umm_json=_granule_dict(g),
            )
        )
    return rows


def run_sample(
    db_path: Path | str,
    n_bins: int = 5,
    only_daac: str | None = None,
    *,
    access: str = "direct",
) -> int:
    """Sample granules for every pending collection. Returns total granules written.

    Re-samples any collection whose existing granule rows were captured under a
    different access mode, so the URL scheme in the granules table always
    matches the requested mode.
    """
    con = connect(db_path)
    init_schema(con)

    # Detect collections with mismatched-mode granules so we can warn the
    # operator about stale rows in the parquet results log before overwriting
    # the granules table.
    stale_query = """
        SELECT DISTINCT collection_concept_id
        FROM granules
        WHERE access_mode != ?
    """
    stale_params: list[Any] = [access]
    if only_daac:
        stale_query += """
            AND collection_concept_id IN (
                SELECT concept_id FROM collections WHERE daac = ?
            )
        """
        stale_params.append(only_daac)
    stale_collections = [
        r[0] for r in con.execute(stale_query, stale_params).fetchall()
    ]

    if stale_collections:
        delete_query = "DELETE FROM granules WHERE access_mode != ?"
        delete_params: list[Any] = [access]
        if only_daac:
            delete_query += """
                AND collection_concept_id IN (
                    SELECT concept_id FROM collections WHERE daac = ?
                )
            """
            delete_params.append(only_daac)
        con.execute(delete_query, delete_params)
        _LOGGER.warning(
            "re-sampling %d collection(s) under access=%r because their granules "
            "table rows were captured under a different mode: %s. "
            "Existing rows in output/results/*.parquet for these collections "
            "still reference the old URLs; if you want attempt to re-fetch them "
            "under the new mode, also delete those parquet rows.",
            len(stale_collections),
            access,
            ", ".join(stale_collections[:10])
            + (" ..." if len(stale_collections) > 10 else ""),
        )

    q = """
        SELECT concept_id, time_start, time_end, num_granules, daac, skip_reason
        FROM collections
        WHERE (skip_reason IS NULL OR skip_reason = 'format_unknown')
          AND concept_id NOT IN (SELECT DISTINCT collection_concept_id FROM granules)
    """
    params: list[Any] = []
    if only_daac:
        q += " AND daac = ?"
        params.append(only_daac)
    colls: list[SampleCollection] = [
        SampleCollection(
            concept_id=r[0],
            time_start=r[1],
            time_end=r[2],
            num_granules=r[3],
            daac=r[4],
            skip_reason=r[5],
        )
        for r in con.execute(q, params).fetchall()
    ]

    total = 0
    for coll in colls:
        if coll["skip_reason"] == "format_unknown":
            # Probe one granule to infer the format.
            probe = earthaccess.search_data(concept_id=coll["concept_id"], count=1)
            fmt = _granule_format(probe[0]) if probe else None
            resolved = _update_collection_classification(con, coll["concept_id"], fmt)
            if resolved is not None:
                continue  # Still unknown or non-array; skip.

        rows = sample_one_collection(coll, n_bins=n_bins, access=access)
        for r in rows:
            con.execute(
                """INSERT OR IGNORE INTO granules
                   (collection_concept_id, granule_concept_id, data_url, https_url,
                    temporal_bin, size_bytes, sampled_at, stratified, access_mode,
                    umm_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    r["collection_concept_id"],
                    r["granule_concept_id"],
                    r["data_url"],
                    r["https_url"],
                    r["temporal_bin"],
                    r["size_bytes"],
                    r["sampled_at"],
                    r["stratified"],
                    r["access_mode"],
                    json.dumps(r["umm_json"]),
                ],
            )
            total += 1
    return total
