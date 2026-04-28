# virtual-zarr-coverage

Tracks VirtualiZarr coverage of cloud-hosted NASA CMR collections, so VirtualiZarr maintainers and NASA DAAC operators can see at a glance which collections are usable as virtual Zarr stores today and which need work.

The pipeline runs in five phases:

1. **Discover** collections from CMR.
2. **Sample** granules stratified across each collection's temporal extent.
3. **Parsability**: can VirtualiZarr produce a `ManifestStore` from a granule URL?
4. **Datasetability** / **Datatreeability**: can the `ManifestStore` be loaded as an `xarray.Dataset` or `xarray.DataTree`?
5. **Cubability**: can per-granule datasets be combined into one coherent virtual store?

Each failure is bucketed into an empirical taxonomy so the long tail can be triaged.

> **What this measures.** Phases 3, 4a, and 4b verify that VirtualiZarr can *construct* a virtual reference and wrap it in xarray — they do not read chunk bytes through the manifest or compare them against the source file. A "successful" granule is constructable, not necessarily readable. See [What's not exercised](https://nasa-impact.github.io/virtual-zarr-coverage/design/architecture/#whats-not-exercised) for the gap and proposed avenues to close it.

## Quick start

```bash
uv sync
uv run vzc discover --top 20
uv run vzc sample --n-bins 2
uv run vzc prefetch
uv run vzc attempt --access external
uv run vzc render
```

Requires Earthdata Login credentials in `~/.netrc`. Drop `prefetch` and use `--access direct` instead when running on AWS us-west-2 compute.

## Documentation

The full documentation site is at <https://nasa-impact.github.io/virtual-zarr-coverage/>:

- [Latest survey results](https://nasa-impact.github.io/virtual-zarr-coverage/results/): figures, taxonomy breakdown, per-DAAC and per-collection rollups.
- [Usage and run modes](https://nasa-impact.github.io/virtual-zarr-coverage/): pilot, per-phase commands, `--access` modes, overrides, reproducing a single failure, granule cache.
- [Glossary](https://nasa-impact.github.io/virtual-zarr-coverage/glossary/) and [failure taxonomy](https://nasa-impact.github.io/virtual-zarr-coverage/design/taxonomy/) for terms and bucket meanings.
- [Architecture](https://nasa-impact.github.io/virtual-zarr-coverage/design/architecture/) for the full design walk-through.
- [Contributing](https://nasa-impact.github.io/virtual-zarr-coverage/contributing/): dev setup, tests, regenerating committed figures, extending the taxonomy.

## License

Distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
