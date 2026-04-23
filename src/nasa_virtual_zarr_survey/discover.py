"""Phase 1 (discover): enumerate cloud-hosted EOSDIS collections into DuckDB."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import earthaccess

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.formats import classify_format
from nasa_virtual_zarr_survey.providers import get_eosdis_providers


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _first_format(coll: dict[str, Any]) -> str | None:
    archive = coll.get("umm", {}).get("ArchiveAndDistributionInformation", {})
    # Prefer FileDistributionInformation (actual distributed format).
    infos = archive.get("FileDistributionInformation") or []
    if isinstance(infos, dict):
        infos = [infos]
    for info in infos:
        fmt = info.get("Format") if isinstance(info, dict) else None
        if fmt:
            return fmt
    # Fall back to FileArchiveInformation (format as archived).
    archive_infos = archive.get("FileArchiveInformation") or []
    if isinstance(archive_infos, dict):
        archive_infos = [archive_infos]
    for info in archive_infos:
        fmt = info.get("Format") if isinstance(info, dict) else None
        if fmt:
            return fmt
    return None


def _first_temporal(umm: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    extents = umm.get("umm", {}).get("TemporalExtents", [])
    for ex in extents:
        rdts = ex.get("RangeDateTimes", [])
        for rdt in rdts:
            return _parse_iso(rdt.get("BeginningDateTime")), _parse_iso(
                rdt.get("EndingDateTime")
            )
    return None, None


def collection_row_from_umm(coll: dict[str, Any]) -> dict[str, Any]:
    """Extract a DuckDB-ready row from a CMR UMM-JSON dict."""
    meta = coll.get("meta", {})
    umm = coll.get("umm", {})
    concept_id = meta.get("concept-id")
    provider = meta.get("provider-id")
    daac = provider
    centers = umm.get("DataCenters", [])
    if centers and isinstance(centers, list):
        daac = centers[0].get("ShortName", provider)
    declared = _first_format(coll)
    family = classify_format(declared, None)
    time_start, time_end = _first_temporal(coll)

    if family is not None:
        skip_reason = None
    elif declared is None:
        skip_reason = "format_unknown"
    else:
        skip_reason = "non_array_format"

    return {
        "concept_id": concept_id,
        "short_name": umm.get("ShortName"),
        "version": umm.get("Version"),
        "daac": daac,
        "provider": provider,
        "format_family": family.value if family else None,
        "format_declared": declared,
        "num_granules": None,
        "time_start": time_start,
        "time_end": time_end,
        "processing_level": (umm.get("ProcessingLevel") or {}).get("Id"),
        "skip_reason": skip_reason,
        "discovered_at": datetime.now(timezone.utc),
    }


def persist_collections(con, rows: Iterable[dict[str, Any]]) -> None:
    """Upsert collection rows into DuckDB."""
    init_schema(con)
    stmt = """
        INSERT OR REPLACE INTO collections
        (concept_id, short_name, version, daac, provider, format_family, format_declared,
         num_granules, time_start, time_end, processing_level, skip_reason, discovered_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for coll in rows:
        row = collection_row_from_umm(coll) if "meta" in coll else coll
        con.execute(
            stmt,
            [
                row["concept_id"],
                row["short_name"],
                row["version"],
                row["daac"],
                row["provider"],
                row["format_family"],
                row["format_declared"],
                row["num_granules"],
                row["time_start"],
                row["time_end"],
                row["processing_level"],
                row["skip_reason"],
                row["discovered_at"],
            ],
        )


def fetch_collection_dicts(
    limit: int | None = None,
    *,
    top_per_provider: int | None = None,
    top_total: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch UMM-JSON dicts for CMR collections. No DB writes.

    Modes (mutually exclusive):
    - Default: all cloud-hosted EOSDIS collections, optionally capped by `limit`.
    - `top_per_provider=N`: top-N per provider by usage_score.
    - `top_total=N`: top-N across providers by usage_score.
    """
    if top_per_provider is not None and top_total is not None:
        raise ValueError("top_per_provider and top_total are mutually exclusive")

    providers = get_eosdis_providers()

    if top_per_provider is not None or top_total is not None:
        from nasa_virtual_zarr_survey.popularity import (
            all_top_collection_ids,
            top_collection_ids_total,
        )

        if top_per_provider is not None:
            ids = all_top_collection_ids(providers, num_per_provider=top_per_provider)
        else:
            ids = top_collection_ids_total(providers, num_total=top_total)
        if not ids:
            return []
        dicts: list[dict] = []
        BATCH = 100
        for i in range(0, len(ids), BATCH):
            batch = ids[i : i + BATCH]
            results = earthaccess.search_datasets(concept_id=batch, count=len(batch))
            dicts.extend(
                c.render_dict if hasattr(c, "render_dict") else c for c in results
            )
        return dicts

    count = -1 if limit is None else limit
    results = earthaccess.search_datasets(
        cloud_hosted=True, provider=providers, count=count
    )
    return [c.render_dict if hasattr(c, "render_dict") else c for c in results]


def run_discover(
    db_path: Path | str,
    limit: int | None = None,
    *,
    top_per_provider: int | None = None,
    top_total: int | None = None,
) -> int:
    """Enumerate EOSDIS collections and persist to DuckDB.

    See `fetch_collection_dicts` for mode semantics.
    """
    con = connect(db_path)
    init_schema(con)
    dicts = fetch_collection_dicts(
        limit=limit,
        top_per_provider=top_per_provider,
        top_total=top_total,
    )
    persist_collections(con, dicts)
    return len(dicts)
