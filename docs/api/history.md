# History page

Render the [Coverage over time](../results/history.md) page from committed `*.summary.json` digests under `docs/results/history/`. Each digest is one snapshot (release or preview); the renderer cross-references them to produce funnel-over-time and bucket-trend charts, a state-transition diff between the two latest releases, and a feature-introductions table sourced from `config/feature_introductions.toml`.

The CLI entry point is `nasa-virtual-zarr-survey history`. The renderer warns (without failing) if the digests disagree on `locked_sample_sha256` — that means the snapshots were not all evaluated against the same locked sample and are not directly comparable.

::: nasa_virtual_zarr_survey.history.run_history
    handler: python
