"""EOSDIS DAAC provider list (snapshot).

Ported from titiler-cmr-compatibility. Re-check annually against
https://cmr.earthdata.nasa.gov/search/providers.
"""

from __future__ import annotations

EOSDIS_PROVIDERS: list[str] = [
    "ASF",
    "ASDC",
    "GES_DISC",
    "GHRC_DAAC",
    "LAADS",
    "LARC_ASDC",
    "LPCLOUD",
    "LPDAAC_ECS",
    "NSIDC_CPRD",
    "NSIDC_ECS",
    "OB_DAAC",
    "ORNL_DAAC",
    "PODAAC",
    "SEDAC",
]


def get_eosdis_providers() -> list[str]:
    """Return a sorted copy of the EOSDIS provider list."""
    return sorted(EOSDIS_PROVIDERS)
