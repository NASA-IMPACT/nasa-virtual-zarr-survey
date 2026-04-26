from nasa_virtual_zarr_survey.providers import EOSDIS_PROVIDERS, get_eosdis_providers


def test_eosdis_providers_is_nonempty():
    assert len(EOSDIS_PROVIDERS) >= 10


def test_eosdis_providers_contains_known_cloud_daacs():
    # Cloud-only EOSDIS provider IDs (the legacy on-prem siblings — PODAAC,
    # NSIDC_ECS, LPDAAC_ECS, ORNL_DAAC, LARC_ASDC — host no cloud collections
    # and intentionally are not in this list).
    required = {"POCLOUD", "NSIDC_CPRD", "LPCLOUD", "GES_DISC", "ASF"}
    assert required.issubset(set(EOSDIS_PROVIDERS))


def test_get_eosdis_providers_returns_sorted_copy():
    result = get_eosdis_providers()
    assert result == sorted(EOSDIS_PROVIDERS)
    # must be a copy, not the module-level list
    result.append("XXXX")
    assert "XXXX" not in EOSDIS_PROVIDERS
