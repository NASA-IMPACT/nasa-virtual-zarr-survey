"""Phase 1 (discover): enumerate cloud-hosted EOSDIS collections into DuckDB."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, cast

import earthaccess

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.formats import classify_format
from nasa_virtual_zarr_survey.opendap import (
    cloud_opendap_service_ids,
    collection_has_cloud_opendap,
)
from nasa_virtual_zarr_survey.providers import get_eosdis_providers
from nasa_virtual_zarr_survey.types import CollectionRow


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


def collection_row_from_umm(coll: dict[str, Any]) -> CollectionRow:
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
    processing_level = (umm.get("ProcessingLevel") or {}).get("Id")
    has_opendap = collection_has_cloud_opendap(coll, cloud_opendap_service_ids())

    # processing_level is recorded for analysis but does not gate sampling:
    # per-granule virtualization (parsability/datasetability) is independent
    # of processing level. Only cubability filters by level (CUBE_MIN_RANK).
    if family is not None:
        skip_reason = None
    elif declared is None:
        skip_reason = "format_unknown"
    else:
        skip_reason = "non_array_format"

    return CollectionRow(
        concept_id=concept_id,
        short_name=umm.get("ShortName"),
        version=umm.get("Version"),
        daac=daac,
        provider=provider,
        format_family=family.value if family else None,
        format_declared=declared,
        num_granules=None,
        time_start=time_start,
        time_end=time_end,
        processing_level=processing_level,
        skip_reason=skip_reason,
        has_cloud_opendap=has_opendap,
        discovered_at=datetime.now(timezone.utc),
        umm_json=coll,
    )


def persist_collections(con, rows: Iterable[dict[str, Any]]) -> None:
    """Upsert collection rows into DuckDB.

    Accepts either raw UMM-JSON dicts (which will be transformed via
    ``collection_row_from_umm``) or already-built ``CollectionRow`` dicts.
    """
    init_schema(con)
    stmt = """
        INSERT OR REPLACE INTO collections
        (concept_id, short_name, version, daac, provider, format_family, format_declared,
         num_granules, time_start, time_end, processing_level, skip_reason,
         has_cloud_opendap, discovered_at, umm_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for coll in rows:
        row: CollectionRow = (
            collection_row_from_umm(coll)
            if "meta" in coll
            else cast(CollectionRow, coll)
        )
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
                row["has_cloud_opendap"],
                row["discovered_at"],
                json.dumps(row["umm_json"]),
            ],
        )


def fetch_collection_dicts(
    limit: int | None = None,
    *,
    top_per_provider: int | None = None,
    top_total: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, tuple[int, int | None]] | None]:
    """Fetch UMM-JSON dicts for CMR collections. No DB writes.

    Returns ``(dicts, score_map)``:

    - ``dicts`` are raw UMM-JSON dicts; see ``collection_row_from_umm`` for the
      typed row shape.
    - ``score_map`` is ``{concept_id: (rank, usage_score)}`` in top-N modes,
      where ``rank`` is 1-based popularity order. ``usage_score`` is ``None``
      when the collection has no entry in CMR's community-usage-metrics
      (notably ESA-distributed Sentinels). ``score_map`` itself is ``None`` in
      non-top modes.

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
            pairs = all_top_collection_ids(providers, num_per_provider=top_per_provider)
        else:
            assert top_total is not None  # guaranteed by the outer condition
            pairs = top_collection_ids_total(providers, num_total=top_total)
        if not pairs:
            return [], {}
        # Build {id: (rank, score)} preserving the score-desc ordering as rank.
        score_map: dict[str, tuple[int, int | None]] = {
            cid: (rank, score) for rank, (cid, score) in enumerate(pairs, start=1)
        }
        ids = [cid for cid, _ in pairs]
        dicts: list[dict] = []
        BATCH = 100
        for i in range(0, len(ids), BATCH):
            batch = ids[i : i + BATCH]
            results = earthaccess.search_datasets(concept_id=batch, count=len(batch))
            dicts.extend(
                c.render_dict if hasattr(c, "render_dict") else c for c in results
            )
        return dicts, score_map

    count = -1 if limit is None else limit
    results = earthaccess.search_datasets(
        cloud_hosted=True, provider=providers, count=count
    )
    dicts = [c.render_dict if hasattr(c, "render_dict") else c for c in results]
    return dicts, None


def sampling_mode_string(
    limit: int | None,
    top_per_provider: int | None,
    top_total: int | None,
) -> str:
    if top_per_provider is not None:
        return f"top-per-provider={top_per_provider}"
    if top_total is not None:
        return f"top={top_total}"
    if limit is not None:
        return f"limit={limit}"
    return "all"


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
    dicts, _score_map = fetch_collection_dicts(
        limit=limit,
        top_per_provider=top_per_provider,
        top_total=top_total,
    )
    persist_collections(con, dicts)
    mode = sampling_mode_string(limit, top_per_provider, top_total)
    con.execute(
        "INSERT OR REPLACE INTO run_meta (key, value, updated_at) VALUES (?, ?, ?)",
        ["sampling_mode", mode, datetime.now(timezone.utc)],
    )
    return len(dicts)
