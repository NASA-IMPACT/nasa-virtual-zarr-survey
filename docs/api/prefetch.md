# prefetch

Phase 2.5: pre-warm the on-disk cache with sampled granules in
popularity-rank order. HTTPS-only — the single writer of the cache.
``attempt --access external`` reads from the cache and fails fast on miss.

The cache directory comes from `NASA_VZ_SURVEY_CACHE_DIR` (env);
default is `~/.cache/nasa-virtual-zarr-survey`. The cap is checked at
collection boundaries: the collection that pushes total cache size past
`cache_max_bytes` finishes writing all its granules and then prefetch
stops. Cached files are never deleted.

::: vzc.prefetch
    handler: python
