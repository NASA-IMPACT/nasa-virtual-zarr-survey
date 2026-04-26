# Discover (Phase 1)

Enumerate CMR collections and write to DuckDB. Supports cloud-hosted enumeration, top-N per provider (`usage_score`-ranked), and top-N total.

The CLI entry point is `nasa-virtual-zarr-survey discover`. `run_discover` is the function it calls; `fetch_collection_dicts` is what you'd call directly from a notebook to inspect raw CMR responses without touching the DB.

::: nasa_virtual_zarr_survey.discover.run_discover
    handler: python

::: nasa_virtual_zarr_survey.discover.fetch_collection_dicts
    handler: python

::: nasa_virtual_zarr_survey.discover.collection_row_from_umm
    handler: python

::: nasa_virtual_zarr_survey.discover.persist_collections
    handler: python
