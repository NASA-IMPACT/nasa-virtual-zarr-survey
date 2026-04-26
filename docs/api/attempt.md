# Attempt (Phases 3 and 4)

Parser dispatch and the open pipeline used during granule attempts: Parsability (Phase 3, build a `ManifestStore`), Datasetability (Phase 4a, `to_virtual_dataset`), Datatreeability (Phase 4b, `to_virtual_datatree`).

The CLI entry point is `nasa-virtual-zarr-survey attempt`. `run_attempt` drives a full pass over sampled granules and writes per-attempt rows to the DAAC-partitioned Parquet log via `ResultWriter`. `attempt_one` is the per-granule unit useful when iterating outside the harness; `dispatch_parser` selects the right `virtualizarr` parser for a given format family.

::: nasa_virtual_zarr_survey.attempt.AttemptResult
    handler: python

::: nasa_virtual_zarr_survey.attempt.dispatch_parser
    handler: python

::: nasa_virtual_zarr_survey.attempt.attempt_one
    handler: python

::: nasa_virtual_zarr_survey.attempt.run_attempt
    handler: python

::: nasa_virtual_zarr_survey.attempt.ResultWriter
    handler: python
