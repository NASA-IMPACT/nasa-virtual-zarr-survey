"""Fetch the top-N most-used collection IDs per EOSDIS provider from CMR."""
from __future__ import annotations

import requests

_CMR_COLLECTIONS_URL = "https://cmr.earthdata.nasa.gov/search/collections.json"
_MAX_PAGE_SIZE = 2000


def top_collection_ids(provider: str, num: int = 100) -> list[str]:
    """Return the top-N collection concept IDs for `provider`, ranked by usage_score."""
    if num > _MAX_PAGE_SIZE:
        raise ValueError(
            f"num must be <= {_MAX_PAGE_SIZE}; CMR does not support paging for this sort"
        )
    response = requests.post(
        _CMR_COLLECTIONS_URL,
        data={
            "provider": provider,
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
    return [entry["id"] for entry in response.json()["feed"]["entry"]]


def all_top_collection_ids(providers: list[str], num_per_provider: int = 100) -> list[str]:
    """Fetch top-N per provider and concatenate. Preserves per-provider ordering."""
    ids: list[str] = []
    for provider in providers:
        try:
            ids.extend(top_collection_ids(provider, num=num_per_provider))
        except requests.HTTPError:
            # Skip providers that fail (e.g., no popularity data for that provider);
            # do not abort the whole run.
            continue
    return ids


def top_collection_ids_total(providers: list[str], num_total: int) -> list[str]:
    """Fetch up to `num_total` collection IDs across providers, ranked by usage_score.

    Divides the budget evenly across providers (ceil), then truncates to exactly
    `num_total` after collecting. Earlier providers in the list contribute first.
    """
    if num_total <= 0 or not providers:
        return []
    import math
    per_provider = math.ceil(num_total / len(providers))
    ids = all_top_collection_ids(providers, num_per_provider=per_provider)
    return ids[:num_total]
