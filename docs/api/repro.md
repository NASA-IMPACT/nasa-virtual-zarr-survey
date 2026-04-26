# Repro

Generates self-contained Python scripts that reproduce a single failing granule outside the survey harness. Reads the data URL, parser kwargs, and any per-collection overrides from the survey state, renders a script, optionally writes it to disk.

The CLI entry point is `nasa-virtual-zarr-survey repro`; see [Reproducing a single failure](../index.md#reproducing-a-single-failure) for the user-facing walk-through. For investigating a concept ID with no recorded failures, see [Probe](probe.md).

::: nasa_virtual_zarr_survey.repro.find_failures
    handler: python

::: nasa_virtual_zarr_survey.repro.generate_script
    handler: python
