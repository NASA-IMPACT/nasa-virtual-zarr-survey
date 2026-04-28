# run

Run one survey snapshot against the currently-prepared environment:
`attempt` + `render --no-render --export ...` against
`config/locked_sample.json`. Each call writes a `*.summary.json` digest
under `docs/results/history/<slug>.summary.json`.

The snapshot's date is read from `[tool.uv] exclude-newer` in
`pyproject.toml` (override with `snapshot_date=`). Pass `label` to mark
the run as a preview (typically used alongside `[tool.uv.sources]` git
overrides on unreleased branches).

::: vzc.run
    handler: python

::: vzc.RunInputs
    handler: python
