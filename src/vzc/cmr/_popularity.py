"""Fetch the top-N most-used collection IDs per EOSDIS provider from CMR.

CMR uses ``usage_score`` as a sort key but does not include the score in the
collections.json response payload. The actual access counts are exposed
separately via the ``community-usage-metrics`` endpoint, keyed by
``(short-name, version)``. We fetch that map once per process (lazy) and join
it onto the per-provider top-N to surface real scores. Collections without a
metrics entry (notably ESA-distributed Sentinels) get ``score=None`` and
render blank in listings.
"""

from __future__ import annotations

import functools

import requests

_CMR_COLLECTIONS_URL = "https://cmr.earthdata.nasa.gov/search/collections.json"
_CMR_METRICS_URL = "https://cmr.earthdata.nasa.gov/search/community-usage-metrics"
_MAX_PAGE_SIZE = 2000


@functools.lru_cache(maxsize=1)
def fetch_usage_metrics() -> dict[tuple[str, str], int]:
    """Return ``{(short_name, version): access_count}`` from CMR's metrics endpoint.

    Cached for the lifetime of the process. Returns an empty dict when the
    endpoint is unreachable so callers degrade to ``score=None`` rather than
    failing the whole top-N run.
    """
    try:
        response = requests.get(_CMR_METRICS_URL, timeout=60)
        response.raise_for_status()
    except requests.RequestException:
        return {}
    return {
        (entry["short-name"], entry.get("version", "N/A")): entry["access-count"]
        for entry in response.json()
    }


def _fetch_provider_top(provider: str, num: int) -> list[tuple[str, str, str]]:
    """Return ``[(concept_id, short_name, version)]`` for `provider`'s top entries.

    Sorted by CMR's internal ``-usage_score``. Filtered to cloud-hosted.
    """
    if num > _MAX_PAGE_SIZE:
        raise ValueError(
            f"num must be <= {_MAX_PAGE_SIZE}; CMR does not support paging for this sort"
        )
    response = requests.post(
        _CMR_COLLECTIONS_URL,
        data={
            "provider": provider,
            "cloud_hosted": True,
            "has_granules_or_cwic": True,
            "include_facets": "v2",
            "include_granule_counts": True,
            "include_has_granules": True,
            "include_tags": "edsc.*,opensearch.granule.osdd",
            "page_num": 1,
            "page_size": num,
            "sort_key[]": "-usage_score",
        },
        timeout=60,
    )
    response.raise_for_status()
    return [
        (entry["id"], entry.get("short_name", ""), entry.get("version_id", "N/A"))
        for entry in response.json()["feed"]["entry"]
    ]


def _attach_score(
    entries: list[tuple[str, str, str]],
    metrics: dict[tuple[str, str], int],
) -> list[tuple[str, int | None]]:
    """Join CMR search entries against the metrics map; ``None`` when no entry."""
    return [
        (cid, metrics.get((short_name, version)))
        for cid, short_name, version in entries
    ]


def top_collection_ids(provider: str, num: int = 100) -> list[tuple[str, int | None]]:
    """Return ``(concept_id, usage_score)`` for `provider`'s top-N cloud-hosted collections.

    Order is CMR's ``-usage_score`` sort. ``usage_score`` is ``None`` for
    collections that have no entry in the community-usage-metrics endpoint.
    """
    entries = _fetch_provider_top(provider, num=num)
    return _attach_score(entries, fetch_usage_metrics())


def all_top_collection_ids(
    providers: list[str], num_per_provider: int = 100
) -> list[tuple[str, int | None]]:
    """Concatenate per-provider top-N, then re-sort by score (descending).

    Each provider contributes up to `num_per_provider` collections (its own
    top-N by usage_score). The merged list is then sorted globally so the
    final rank reflects descending ``usage_score``. Providers that 5xx are
    skipped silently — same intent as before — so a single bad DAAC doesn't
    abort the run.
    """
    metrics = fetch_usage_metrics()
    pool: list[tuple[str, int | None]] = []
    for provider in providers:
        try:
            entries = _fetch_provider_top(provider, num=num_per_provider)
        except requests.HTTPError:
            continue
        pool.extend(_attach_score(entries, metrics))
    return _sort_by_score_desc(pool)


def top_collection_ids_total(
    providers: list[str], num_total: int
) -> list[tuple[str, int | None]]:
    """Return the global top-`num_total` ``(concept_id, usage_score)`` across providers.

    Asks each provider for its top-`num_total` (capped at CMR's 2000 page size),
    merges, sorts by score descending, and truncates. This is a true global
    top-N: a single popular provider can dominate. Collections without a
    metrics entry sort last (treated as score 0 for ordering purposes).
    """
    if num_total <= 0 or not providers:
        return []
    cap = min(num_total, _MAX_PAGE_SIZE)
    metrics = fetch_usage_metrics()
    pool: list[tuple[str, int | None]] = []
    for provider in providers:
        try:
            entries = _fetch_provider_top(provider, num=cap)
        except requests.HTTPError:
            continue
        pool.extend(_attach_score(entries, metrics))
    return _sort_by_score_desc(pool)[:num_total]


def _sort_by_score_desc(
    pairs: list[tuple[str, int | None]],
) -> list[tuple[str, int | None]]:
    """Sort by score descending; ``None`` scores sort last; ties broken by concept_id."""
    return sorted(pairs, key=lambda t: (-(t[1] or 0), t[0]))
