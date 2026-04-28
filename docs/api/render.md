# render

Phase 5 + render: read survey state plus the Parquet log, compute
verdicts, and write the report.

Reads `output/state.json` and `output/results/`. Writes
`docs/results/index.md` plus figure assets under `docs/results/figures/`.
Idempotent and cheap: re-run after refining `taxonomy.py` to update the
Markdown without re-running `attempt`.

::: vzc.render.render
    handler: python
