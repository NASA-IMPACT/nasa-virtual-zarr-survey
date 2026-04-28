"""EOSDIS DAAC provider list (cloud-hosted snapshot).

Snapshot of CMR provider IDs that publish cloud-hosted EOSDIS collections.
Re-check periodically against
``https://cmr.earthdata.nasa.gov/search/collections.umm_json``
filtered by ``cloud_hosted=True&consortium=EOSDIS`` (group by
``meta.provider-id``).

Several DAACs run a legacy on-prem provider plus a separate cloud-only one
(e.g. PODAAC → POCLOUD, ORNL_DAAC → ORNL_CLOUD, LARC_ASDC → LARC_CLOUD,
LPDAAC_ECS → LPCLOUD, NSIDC_ECS → NSIDC_CPRD, OB_DAAC → OB_CLOUD); the cloud
collections live under the cloud-only provider ID, so the legacy IDs would
contribute zero rows to a cloud-hosted survey and are excluded.
"""

from __future__ import annotations

EOSDIS_PROVIDERS: list[str] = [
    "ASF",
    "GES_DISC",
    "GHRC_DAAC",
    "LAADS",
    "LARC_CLOUD",
    "LPCLOUD",
    "NSIDC_CPRD",
    "OB_CLOUD",
    "OB_DAAC",
    "ORNL_CLOUD",
    "POCLOUD",
]


def get_eosdis_providers() -> list[str]:
    """Return a sorted copy of the EOSDIS provider list."""
    return sorted(EOSDIS_PROVIDERS)
