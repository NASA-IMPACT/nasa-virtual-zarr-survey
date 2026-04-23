# Discover (Phase 1)

Enumerate CMR collections and write to DuckDB. Supports cloud-hosted enumeration, top-N per provider (`usage_score`-ranked), and top-N total.

::: nasa_virtual_zarr_survey.discover.run_discover
    handler: python

::: nasa_virtual_zarr_survey.discover.fetch_collection_dicts
    handler: python

::: nasa_virtual_zarr_survey.discover.collection_row_from_umm
    handler: python

::: nasa_virtual_zarr_survey.discover.persist_collections
    handler: python
