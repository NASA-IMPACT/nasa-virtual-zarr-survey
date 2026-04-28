"""Phase 1 (discover): enumerate cloud-hosted EOSDIS collections into ``state.json``."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import earthaccess

from vzc.core.formats import classify_format
from vzc.cmr._opendap import (
    cloud_opendap_service_ids,
    collection_has_cloud_opendap,
)
from vzc.cmr._providers import get_eosdis_providers
from vzc.state._io import (
    CollectionRow,
    load_state,
    save_state,
    upsert_collections,
)


def _parse_iso(s: str | None) -> str | None:
    """Normalize a CMR ISO timestamp to a single ``isoformat`` string."""
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).isoformat()
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


def _first_temporal(coll: dict[str, Any]) -> tuple[str | None, str | None]:
    extents = coll.get("umm", {}).get("TemporalExtents", [])
    for ex in extents:
        rdts = ex.get("RangeDateTimes", [])
        for rdt in rdts:
            return _parse_iso(rdt.get("BeginningDateTime")), _parse_iso(
                rdt.get("EndingDateTime")
            )
    return None, None


def collection_row_from_umm(coll: dict[str, Any]) -> CollectionRow:
    """Extract a :class:`CollectionRow` from a CMR UMM-JSON dict."""
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
        popularity_rank=None,
        usage_score=None,
        discovered_at=datetime.now(timezone.utc).isoformat(),
        umm_json=coll,
    )


def fetch_collection_dicts(
    limit: int | None = None,
    *,
    top_per_provider: int | None = None,
    top_total: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, tuple[int, int | None]] | None]:
    """Fetch UMM-JSON dicts for CMR collections. No state writes.

    Returns ``(dicts, score_map)``:

    - ``dicts`` are raw UMM-JSON dicts; see :func:`collection_row_from_umm`
      for the typed row shape.
    - ``score_map`` is ``{concept_id: (rank, usage_score)}`` in top-N modes,
      where ``rank`` is 1-based popularity order. ``usage_score`` is ``None``
      when the collection has no entry in CMR's community-usage-metrics
      (notably ESA-distributed Sentinels). ``score_map`` itself is ``None`` in
      non-top modes.

    Modes (mutually exclusive):

    - Default: all cloud-hosted EOSDIS collections, optionally capped by ``limit``.
    - ``top_per_provider=N``: top-N per provider by ``usage_score``.
    - ``top_total=N``: top-N across providers by ``usage_score``.
    """
    if top_per_provider is not None and top_total is not None:
        raise ValueError("top_per_provider and top_total are mutually exclusive")

    providers = get_eosdis_providers()

    if top_per_provider is not None or top_total is not None:
        from vzc.cmr._popularity import (
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


def build_collection_rows(
    dicts: list[dict[str, Any]],
    *,
    score_map: dict[str, tuple[int, int | None]] | None = None,
) -> list[CollectionRow]:
    """Turn UMM-JSON dicts into :class:`CollectionRow` instances.

    When ``score_map`` is provided (top-N discovery modes), each row's
    ``popularity_rank`` and ``usage_score`` come from the map.
    """
    rows: list[CollectionRow] = []
    for coll in dicts:
        row = collection_row_from_umm(coll)
        if score_map is not None and row.concept_id in score_map:
            rank, score = score_map[row.concept_id]
            row.popularity_rank = rank
            row.usage_score = score
        rows.append(row)
    return rows


def discover(
    *,
    limit: int | None = None,
    top: int | None = None,
    top_per_provider: int | None = None,
) -> int:
    """Enumerate EOSDIS collections and persist to ``output/state.json``.

    Reads / writes ``output/state.json`` (relative to the current working
    directory). See :func:`fetch_collection_dicts` for the three mutually-
    exclusive scope modes (``limit``, ``top``, ``top_per_provider``).
    Returns the number of collections written.
    """
    from vzc._config import DEFAULT_STATE_PATH

    dicts, score_map = fetch_collection_dicts(
        limit=limit,
        top_per_provider=top_per_provider,
        top_total=top,
    )
    rows = build_collection_rows(dicts, score_map=score_map)

    state = load_state(DEFAULT_STATE_PATH)
    upsert_collections(state, rows)
    state.run_meta["sampling_mode"] = sampling_mode_string(limit, top_per_provider, top)
    save_state(state, DEFAULT_STATE_PATH)
    return len(rows)
