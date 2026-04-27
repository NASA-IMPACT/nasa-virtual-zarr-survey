"""Phase 2 (sample): for each collection, pick N granules stratified across CMR's revision_date ordering."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import earthaccess
import requests
from earthaccess.results import DataGranule

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.opendap import dmrpp_url_for, verify_dmrpp_exists
from nasa_virtual_zarr_survey.types import GranuleInfo, SampleCollection


_LOGGER = logging.getLogger(__name__)

_CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"


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


def _resolve_dmrpp_url(
    https_url: str | None, has_opendap: bool, verify: bool
) -> str | None:
    """Compute (and optionally HEAD-verify) the ``.dmrpp`` URL for a granule.

    Returns ``None`` for collections without a cloud-OPeNDAP UMM-S association
    or when verification is requested and the sidecar is absent. The
    constructed URL pins to ``https_url`` so it's curl-able from anywhere.
    """
    if not has_opendap:
        return None
    url = dmrpp_url_for(https_url)
    if url is None:
        return None
    if verify and not verify_dmrpp_exists(url):
        return None
    return url


def _hits(concept_id: str) -> int:
    """Return the live granule count for `concept_id` from CMR.

    One request with ``page_size=0``; the count comes from the
    ``cmr-hits`` response header. Used as a lazy fallback when
    ``collections.num_granules`` is missing.
    """
    params: dict[str, str | int] = {
        "collection_concept_id": concept_id,
        "page_size": 0,
    }
    response = requests.get(_CMR_GRANULES_URL, params=params, timeout=60)
    response.raise_for_status()
    return int(response.headers["cmr-hits"])


def _is_cloud_hosted(umm: dict[str, Any]) -> bool:
    """Mirror of ``earthaccess.search.DataGranules._is_cloud_hosted``.

    Inlined so we can wrap raw CMR JSON in a ``DataGranule`` with the
    correct flag without going through the ``earthaccess.search_data``
    code path.
    """
    if "RelatedUrls" not in umm:
        return False
    for link in umm["RelatedUrls"]:
        if (
            "protected" in link.get("URL", "")
            or link.get("Type") == "GET DATA VIA DIRECT ACCESS"
        ):
            return True
    return False


def _fetch_at_offset(concept_id: str, offset: int) -> DataGranule | None:
    """Fetch the single granule at the given positional offset.

    Sorts by ``revision_date`` ascending so offset 0 is the oldest revision.
    Returns ``None`` if the response carries no items (race with deletion,
    or pagination edge with concurrent ingests).
    """
    params: dict[str, str | int] = {
        "collection_concept_id": concept_id,
        "sort_key": "revision_date",
        "page_size": 1,
        "page_num": offset + 1,  # CMR pages are 1-indexed
    }
    response = requests.get(_CMR_GRANULES_URL, params=params, timeout=60)
    response.raise_for_status()
    items = response.json().get("items") or []
    if not items:
        return None
    raw = items[0]
    return DataGranule(raw, cloud_hosted=_is_cloud_hosted(raw["umm"]))


def _fetch_with_retry(
    concept_id: str, offset: int, *, bin_index: int
) -> DataGranule | None:
    """Fetch a granule at `offset`; on empty response, retry once at the adjacent offset.

    Adjacent direction: ``offset - 1`` normally, or ``offset + 1`` when
    ``offset == 0``. On second failure, log a warning and return ``None``;
    caller drops the bin.
    """
    g = _fetch_at_offset(concept_id, offset)
    if g is not None:
        return g
    retry_offset = offset + 1 if offset == 0 else offset - 1
    g = _fetch_at_offset(concept_id, retry_offset)
    if g is not None:
        return g
    _LOGGER.warning(
        "collection %s bin %d (offset=%d) returned no granule after retry",
        concept_id,
        bin_index,
        offset,
    )
    return None


def sample_one_collection(
    coll: SampleCollection,
    n_bins: int = 5,
    *,
    access: str = "direct",
    verify_dmrpp: bool = False,
) -> list[GranuleInfo]:
    """Return up to `n_bins` granule rows for `coll`, stratified across CMR's
    ``revision_date`` ordering.

    For collections with ``num_granules`` granules, fetches the granules at
    offsets ``[i * num_granules // n_bins for i in range(n_bins)]`` against the
    CMR ``granules.umm_json`` endpoint sorted by ``revision_date`` ascending.

    When ``coll['num_granules']`` is ``None``, calls ``_hits`` once to fetch
    the live count from CMR.

    When the population is smaller than ``n_bins``, fetches every granule
    (offsets ``range(num_granules)``).

    On a CMR query that returns no granule for a bin (rare race condition with
    granule deletion or concurrent ingest), retries once at the adjacent
    offset; on second failure logs a warning and skips the bin.
    """
    concept_id = coll["concept_id"]
    n_total = coll.get("num_granules")
    if n_total is None:
        n_total = _hits(concept_id)
    if n_total == 0:
        return []
    if n_total <= n_bins:
        offsets = list(range(n_total))
    else:
        offsets = [i * n_total // n_bins for i in range(n_bins)]

    has_opendap = bool(coll.get("has_cloud_opendap"))
    now = datetime.now(timezone.utc)
    rows: list[GranuleInfo] = []

    for bin_index, offset in enumerate(offsets):
        g = _fetch_with_retry(concept_id, offset, bin_index=bin_index)
        if g is None:
            continue
        data_url, https_url = _extract_urls(g, access)
        rows.append(
            GranuleInfo(
                collection_concept_id=concept_id,
                granule_concept_id=g["meta"]["concept-id"],
                data_url=data_url,
                https_url=https_url,
                dmrpp_granule_url=_resolve_dmrpp_url(
                    https_url, has_opendap, verify_dmrpp
                ),
                stratification_bin=bin_index,
                n_total_at_sample=n_total,
                size_bytes=_extract_size(g),
                sampled_at=now,
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
    verify_dmrpp: bool = False,
) -> int:
    """Sample granules for every pending collection. Returns total granules written.

    Re-samples any collection whose existing granule rows were captured under a
    different access mode, so the URL scheme in the granules table always
    matches the requested mode. With ``verify_dmrpp=True``, every constructed
    ``.dmrpp`` URL is HEAD-checked against the upstream object store and
    nulled out on 404 — costs one extra request per sampled granule.
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
        SELECT concept_id, time_start, time_end, num_granules, daac, skip_reason,
               has_cloud_opendap
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
            has_cloud_opendap=bool(r[6]),
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

        rows = sample_one_collection(
            coll, n_bins=n_bins, access=access, verify_dmrpp=verify_dmrpp
        )
        for r in rows:
            con.execute(
                """INSERT OR IGNORE INTO granules
                   (collection_concept_id, granule_concept_id, data_url, https_url,
                    dmrpp_granule_url, stratification_bin, n_total_at_sample, size_bytes,
                    sampled_at, access_mode, umm_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    r["collection_concept_id"],
                    r["granule_concept_id"],
                    r["data_url"],
                    r["https_url"],
                    r["dmrpp_granule_url"],
                    r["stratification_bin"],
                    r["n_total_at_sample"],
                    r["size_bytes"],
                    r["sampled_at"],
                    r["access_mode"],
                    json.dumps(r["umm_json"]),
                ],
            )
            total += 1
    return total
