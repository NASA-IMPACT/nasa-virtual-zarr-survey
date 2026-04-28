# attempt

Phases 3 and 4: parsability + datasetability + datatreeability per
granule. Writes Parquet shards under `output/results/`.

With `access="external"`, reads granule bytes from the cache at
`NASA_VZ_SURVEY_CACHE_DIR` and fails fast on miss — run `prefetch` first.
With `access="direct"` the cache is unused.

::: vzc.attempt
    handler: python

::: vzc.AttemptResult
    handler: python
