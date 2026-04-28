"""Phase 2 (sample): for each collection, pick N granules stratified across CMR's revision_date ordering.

Always records both ``s3_url`` (for ``--access direct``) and ``https_url``
(for ``--access external``) per granule, so attempt can flip access modes
without re-sampling.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import earthaccess
import requests
from earthaccess.results import DataGranule

from vzc.cmr._opendap import dmrpp_url_for, verify_dmrpp_exists
from vzc.state._io import (
    CollectionRow,
    GranuleRow,
    SurveyState,
    load_state,
    save_state,
    upsert_granules,
)
from vzc.core.types import SampleCollection


_LOGGER = logging.getLogger(__name__)

_CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

_CMR_OFFSET_CAP = 1_000_000
"""CMR's hard limit: ``page_num * page_size <= 1_000_000``. With ``page_size=1`` this
caps positional access at offset 999_999. Collections with more granules than this
are stratified across only the newest ``_CMR_OFFSET_CAP`` revisions — see
``sample_one_collection``."""


def _first_url(g: DataGranule, *, access: str) -> str | None:
    for link in g.data_links(access=access) or []:
        return link
    return None


def _extract_urls(g: DataGranule) -> tuple[str | None, str | None]:
    """Return ``(s3_url, https_url)`` for the granule, regardless of run mode."""
    return _first_url(g, access="direct"), _first_url(g, access="external")


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


def _reclassify_collection(
    coll: CollectionRow, format_declared: str | None
) -> str | None:
    """Re-classify a collection in-place using a freshly-discovered format string.

    Returns the resolved ``skip_reason`` (``None`` if array-like, else a string).
    """
    from vzc.core.formats import classify_format

    family = classify_format(format_declared, None)
    if family is not None:
        coll.skip_reason = None
        coll.format_family = family.value
    elif format_declared is None:
        coll.skip_reason = "format_unknown"
        coll.format_family = None
    else:
        coll.skip_reason = "non_array_format"
        coll.format_family = None
    coll.format_declared = format_declared
    return coll.skip_reason


def _granule_dict(g: DataGranule) -> dict[str, Any]:
    """Return the full UMM-JSON dict (``{meta, umm}``) for a granule."""
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


def _resolve_dmrpp_url(
    https_url: str | None, has_opendap: bool, verify: bool
) -> str | None:
    if not has_opendap:
        return None
    url = dmrpp_url_for(https_url)
    if url is None:
        return None
    if verify and not verify_dmrpp_exists(url):
        return None
    return url


def _hits(concept_id: str) -> int:
    """Return the live granule count for ``concept_id`` from CMR."""
    params: dict[str, str | int] = {
        "collection_concept_id": concept_id,
        "page_size": 0,
    }
    response = requests.get(_CMR_GRANULES_URL, params=params, timeout=60)
    response.raise_for_status()
    return int(response.headers["cmr-hits"])


def _is_cloud_hosted(umm: dict[str, Any]) -> bool:
    if "RelatedUrls" not in umm:
        return False
    for link in umm["RelatedUrls"]:
        if (
            "protected" in link.get("URL", "")
            or link.get("Type") == "GET DATA VIA DIRECT ACCESS"
        ):
            return True
    return False


def _fetch_at_offset(
    concept_id: str, offset: int, *, sort_key: str = "revision_date"
) -> DataGranule | None:
    params: dict[str, str | int] = {
        "collection_concept_id": concept_id,
        "sort_key": sort_key,
        "page_size": 1,
        "page_num": offset + 1,
    }
    response = requests.get(_CMR_GRANULES_URL, params=params, timeout=60)
    response.raise_for_status()
    items = response.json().get("items") or []
    if not items:
        return None
    raw = items[0]
    return DataGranule(raw, cloud_hosted=_is_cloud_hosted(raw["umm"]))


def _fetch_with_retry(
    concept_id: str, offset: int, *, sort_key: str, bin_index: int
) -> DataGranule | None:
    g = _fetch_at_offset(concept_id, offset, sort_key=sort_key)
    if g is not None:
        return g
    retry_offset = offset + 1 if offset == 0 else offset - 1
    g = _fetch_at_offset(concept_id, retry_offset, sort_key=sort_key)
    if g is not None:
        return g
    _LOGGER.warning(
        "collection %s bin %d (offset=%d, sort_key=%s) returned no granule after retry",
        concept_id,
        bin_index,
        offset,
        sort_key,
    )
    return None


def sample_one_collection(
    coll: SampleCollection,
    n_bins: int = 5,
    *,
    verify_dmrpp: bool = False,
) -> list[GranuleRow]:
    """Return up to ``n_bins`` granule rows for ``coll``, stratified across CMR's
    ``revision_date`` ordering.

    Always records both ``s3_url`` and ``https_url`` per granule.
    """
    concept_id = coll["concept_id"]
    n_total = coll.get("num_granules")
    if n_total is None:
        n_total = _hits(concept_id)
    if n_total == 0:
        return []

    if n_total <= n_bins:
        plan = [(i, "revision_date", i) for i in range(n_total)]
    else:
        plan = [(0, "revision_date", 0)]
        effective = min(n_total, _CMR_OFFSET_CAP)
        desc_offsets = [i * effective // (n_bins - 1) for i in range(n_bins - 1)]
        for bin_index, offset in zip(range(1, n_bins), reversed(desc_offsets)):
            plan.append((bin_index, "-revision_date", offset))

    has_opendap = bool(coll.get("has_cloud_opendap"))
    now = datetime.now(timezone.utc).isoformat()
    rows: list[GranuleRow] = []

    for bin_index, sort_key, offset in plan:
        g = _fetch_with_retry(
            concept_id, offset, sort_key=sort_key, bin_index=bin_index
        )
        if g is None:
            continue
        s3_url, https_url = _extract_urls(g)
        rows.append(
            GranuleRow(
                collection_concept_id=concept_id,
                granule_concept_id=g["meta"]["concept-id"],
                s3_url=s3_url,
                https_url=https_url,
                dmrpp_granule_url=_resolve_dmrpp_url(
                    https_url, has_opendap, verify_dmrpp
                ),
                stratification_bin=bin_index,
                n_total_at_sample=n_total,
                size_bytes=_extract_size(g),
                sampled_at=now,
                umm_json=_granule_dict(g),
            )
        )
    return rows


def _sample_collection_view(coll: CollectionRow) -> SampleCollection:
    return SampleCollection(
        concept_id=coll.concept_id,
        time_start=coll.time_start,
        time_end=coll.time_end,
        num_granules=coll.num_granules,
        daac=coll.daac,
        skip_reason=coll.skip_reason,
        has_cloud_opendap=coll.has_cloud_opendap,
    )


def sample(*, n_bins: int = 5) -> int:
    """Sample granules for every pending collection. Returns total granules written.

    Reads / writes ``output/state.json`` (relative to cwd). Pending
    collections are those whose ``skip_reason`` is null (or
    ``format_unknown``, in which case one granule is fetched first to infer
    the format) and which have no granule rows yet.
    """
    from vzc._config import DEFAULT_STATE_PATH

    state = load_state(DEFAULT_STATE_PATH)
    sampled_ids = {g.collection_concept_id for g in state.granules}

    total = 0
    new_granules: list[GranuleRow] = []
    for coll in state.collections:
        if coll.concept_id in sampled_ids:
            continue
        if coll.skip_reason not in (None, "format_unknown"):
            continue

        if coll.skip_reason == "format_unknown":
            probe = earthaccess.search_data(concept_id=coll.concept_id, count=1)
            fmt = _granule_format(probe[0]) if probe else None
            resolved = _reclassify_collection(coll, fmt)
            if resolved is not None:
                continue  # Still unknown or non-array; skip.

        rows = sample_one_collection(_sample_collection_view(coll), n_bins=n_bins)
        new_granules.extend(rows)
        total += len(rows)

    upsert_granules(state, new_granules)
    save_state(state, DEFAULT_STATE_PATH)
    return total


__all__ = [
    "SurveyState",
    "sample",
    "sample_one_collection",
]
