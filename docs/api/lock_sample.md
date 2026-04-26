# Lock sample

Write a deterministic JSON snapshot of the current DuckDB's collections and granules to `config/locked_sample.json`. The committed file is the fixed comparison set every snapshot is evaluated against; re-run only when you intentionally want to change which (collection, granule) pairs are surveyed.

The CLI entry point is `nasa-virtual-zarr-survey lock-sample`.

::: nasa_virtual_zarr_survey.lock_sample.write_locked_sample
    handler: python
