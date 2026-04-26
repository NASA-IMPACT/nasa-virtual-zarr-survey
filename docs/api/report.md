# Report (Phase 5 and render)

Roll up per-collection verdicts across phases 3, 4a, 4b, run the Phase 5 cubability check on fingerprints, classify failures into taxonomy buckets, and render `docs/results/index.md` with embedded figures.

The CLI entry point is `nasa-virtual-zarr-survey report`. `run_report` is the full driver (DuckDB + Parquet in, Markdown + figures out). `collection_verdicts` returns the per-collection verdict rows for downstream tools, and `render_report` is the pure-Markdown formatter you can call against pre-computed inputs (e.g. when regenerating from `summary.json`).

::: nasa_virtual_zarr_survey.report.run_report
    handler: python

::: nasa_virtual_zarr_survey.report.collection_verdicts
    handler: python

::: nasa_virtual_zarr_survey.report.render_report
    handler: python
