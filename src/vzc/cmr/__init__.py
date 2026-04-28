"""NASA CMR access: discover, sample, popularity ranking, and OPeNDAP detection."""

from vzc.cmr._discover import (
    build_collection_rows,
    collection_row_from_umm,
    discover,
    fetch_collection_dicts,
    sampling_mode_string,
)
from vzc.cmr._opendap import (
    cloud_opendap_service_ids,
    collection_has_cloud_opendap,
    dmrpp_url_for,
    verify_dmrpp_exists,
)
from vzc.cmr._popularity import (
    all_top_collection_ids,
    top_collection_ids_total,
)
from vzc.cmr._providers import get_eosdis_providers
from vzc.cmr._sample import sample, sample_one_collection

__all__ = [
    "all_top_collection_ids",
    "build_collection_rows",
    "cloud_opendap_service_ids",
    "collection_has_cloud_opendap",
    "collection_row_from_umm",
    "discover",
    "dmrpp_url_for",
    "fetch_collection_dicts",
    "get_eosdis_providers",
    "sample",
    "sample_one_collection",
    "sampling_mode_string",
    "top_collection_ids_total",
    "verify_dmrpp_exists",
]
