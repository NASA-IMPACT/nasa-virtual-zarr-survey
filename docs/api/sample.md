# Sample (Phase 2)

Positional-stratification granule sampling plus sample-time re-classification of `format_unknown` collections.

The CLI entry point is `nasa-virtual-zarr-survey sample`. `run_sample` drives the full phase; `sample_one_collection` is the per-collection unit (useful when iterating on one collection in a notebook).

For collections discovered with `has_cloud_opendap=True`, each sampled granule's `dmrpp_granule_url` is recorded as `https_url + ".dmrpp"`. By default the URL is constructed without a network check (the collection's UMM-S association is treated as authoritative). Pass `--verify-dmrpp` to HEAD-check every sidecar against its upstream object store and null out missing ones — a one-time audit; the flag costs one extra request per sampled granule.

```bash
# default: trust the UMM-S association
uv run nasa-virtual-zarr-survey sample --n-bins 5

# audit: verify each .dmrpp sidecar actually exists
uv run nasa-virtual-zarr-survey sample --n-bins 5 --verify-dmrpp
```

::: nasa_virtual_zarr_survey.sample.run_sample
    handler: python

::: nasa_virtual_zarr_survey.sample.sample_one_collection
    handler: python

## DMR++ helpers

Cloud OPeNDAP detection and sidecar URL construction live in a separate module so `discover` and `sample` can share them.

::: nasa_virtual_zarr_survey.opendap.cloud_opendap_service_ids
    handler: python

::: nasa_virtual_zarr_survey.opendap.collection_has_cloud_opendap
    handler: python

::: nasa_virtual_zarr_survey.opendap.dmrpp_url_for
    handler: python

::: nasa_virtual_zarr_survey.opendap.verify_dmrpp_exists
    handler: python
