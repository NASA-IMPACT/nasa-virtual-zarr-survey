# Probe

Generates self-contained Python scripts for investigating a CMR collection or granule when `repro` cannot help — primarily collections that were skipped at discover time (`skip_reason='format_unknown'`, no granules attempted, nothing in the Parquet log) but also any concept ID an operator wants to poke regardless of its survey state.

The CLI entry point is `nasa-virtual-zarr-survey probe`; see [Probing a collection or granule](../index.md#probing-a-collection-or-granule) for the user-facing walk-through.

::: nasa_virtual_zarr_survey.probe.ProbeTarget
    handler: python

::: nasa_virtual_zarr_survey.probe.resolve_target
    handler: python

::: nasa_virtual_zarr_survey.probe.generate_script
    handler: python
