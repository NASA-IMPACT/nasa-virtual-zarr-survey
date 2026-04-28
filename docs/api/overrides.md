# Collection overrides

Per-collection knobs threaded through `attempt` and into
`investigate --mode virtual` repro scripts. Loaded from
`config/collection_overrides.toml`; see [the overrides design](../design/architecture.md#per-collection-overrides)
for the configuration schema and validation rules.

::: vzc.CollectionOverride
    handler: python

::: vzc.OverrideRegistry
    handler: python
