# Sample (Phase 2)

Stratified temporal granule sampling plus sample-time re-classification of `format_unknown` collections.

The CLI entry point is `nasa-virtual-zarr-survey sample`. `run_sample` drives the full phase; `sample_one_collection` is the per-collection unit (useful when iterating on one collection in a notebook); `temporal_bins` is the binning helper exposed for testing.

::: nasa_virtual_zarr_survey.sample.run_sample
    handler: python

::: nasa_virtual_zarr_survey.sample.sample_one_collection
    handler: python

::: nasa_virtual_zarr_survey.sample.temporal_bins
    handler: python
