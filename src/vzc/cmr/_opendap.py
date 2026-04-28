"""Detect cloud OPeNDAP / DMR++ availability via CMR UMM-S associations.

A NASA collection is considered to have cloud DMR++ available when at least
one of its associated UMM-S service records is type ``opendap`` and points at
the Hyrax-in-the-Cloud URL (``https://opendap.earthdata.nasa.gov``). The
opendap repo (TRT-525) catalogues several such records — one official, plus
'unofficial' duplicates from in-flight migrations. Treating them as equivalent
matches operational reality: granules under any of them get a ``.dmrpp``
sidecar in S3 next to the data file, which is what VirtualiZarr's
``DMRPPParser`` actually reads.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import requests

CLOUD_OPENDAP_URL = "https://opendap.earthdata.nasa.gov"
CMR_SERVICES_URL = "https://cmr.earthdata.nasa.gov/search/services.umm_json"


@lru_cache(maxsize=1)
def cloud_opendap_service_ids() -> frozenset[str]:
    """Return the UMM-S concept IDs for cloud-OPeNDAP service records.

    Queries CMR's services endpoint for every record of ``type=opendap``
    and keeps the ones whose URL matches ``CLOUD_OPENDAP_URL``. Cached for
    the process so a single ``discover`` run pays one CMR call.
    """
    r = requests.get(
        CMR_SERVICES_URL,
        params=[("type", "opendap"), ("page_size", "200")],
        timeout=20,
    )
    r.raise_for_status()
    ids: set[str] = set()
    for item in r.json().get("items", []):
        url = (item.get("umm", {}).get("URL", {}).get("URLValue") or "").rstrip("/")
        if url == CLOUD_OPENDAP_URL:
            ids.add(item["meta"]["concept-id"])
    return frozenset(ids)


def collection_has_cloud_opendap(
    coll: dict[str, Any], service_ids: frozenset[str]
) -> bool:
    """True iff the collection's UMM-S associations include any cloud-OPeNDAP record."""
    services = coll.get("meta", {}).get("associations", {}).get("services") or []
    return any(s in service_ids for s in services)


def dmrpp_url_for(data_url: str | None) -> str | None:
    """Construct the ``.dmrpp`` sidecar URL alongside a granule data URL.

    Hyrax-in-the-Cloud writes the sidecar next to the data file with a literal
    ``.dmrpp`` suffix (``.../granule.h5`` → ``.../granule.h5.dmrpp``). This is
    what ``virtualizarr.parsers.dmrpp.DMRPPParser`` reads — no Hyrax round-trip.
    Returns ``None`` for falsy input so callers can pass through optional URLs.
    """
    if not data_url:
        return None
    return data_url + ".dmrpp"


def verify_dmrpp_exists(
    url: str, *, session: requests.Session | None = None, timeout: float = 10.0
) -> bool:
    """HEAD the ``.dmrpp`` URL to confirm the sidecar actually exists.

    Treats any 2xx as present, anything else (including network errors) as
    absent. Use sparingly — one request per granule adds up at survey scale.
    """
    sess = session or requests
    try:
        resp = sess.head(url, timeout=timeout, allow_redirects=True)
    except requests.RequestException:
        return False
    return 200 <= resp.status_code < 300
