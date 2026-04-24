# nasa-virtual-zarr-survey

Surveys cloud-hosted NASA CMR collections for VirtualiZarr compatibility. The pipeline runs as five phases:

1. **Discover** (Phase 1): enumerate CMR collections into DuckDB.
2. **Sample** (Phase 2): pick N granules per collection, stratified across its temporal extent.
3. **Parsability** (Phase 3): the VirtualiZarr parser can produce a `ManifestStore` from a granule URL.
4a. **Datasetability** (Phase 4a): the `ManifestStore` can be converted to an `xarray.Dataset`.
4b. **Datatreeability** (Phase 4b): the `ManifestStore` can be converted to an `xarray.DataTree`. Attempted in parallel with 4a; captures hierarchical files that fail 4a with `CONFLICTING_DIM_SIZES`.
5. **Virtual Store Feasibility / Cubability** (Phase 5): the per-granule datasets can be combined into a coherent virtual store. Gated on Phase 4a `all_pass` (tree-only collections are not yet cubable).

Failures in each phase are categorized into an empirically-derived taxonomy so VirtualiZarr maintainers and NASA DAAC operators can prioritize gaps.

## Getting started

```bash
uv sync
```

Requires Earthdata Login credentials in `~/.netrc`.

### Pilot run

Start small. The pilot runs discover → sample → attempt → report on a bounded set of collections so you can review raw errors and refine the failure taxonomy before committing to a full survey:

```bash
uv run nasa-virtual-zarr-survey pilot --top 20 --n-bins 2 --access external
```

- `--top N` surveys the top-N most-used collections across EOSDIS providers (ranked by CMR's `usage_score`). Swap for `--top-per-provider N` to survey N per provider.
- `--n-bins 2` samples 2 granules stratified across each collection's temporal extent (default is 5).
- `--access external` uses HTTPS URLs with an EDL bearer token so the tool works outside AWS us-west-2. Use `--access direct` when running on AWS us-west-2 compute for direct S3 access.

### Per-phase commands

The pilot is a convenience wrapper; the full pipeline is also available one phase at a time:

```bash
uv run nasa-virtual-zarr-survey discover --top 200
uv run nasa-virtual-zarr-survey sample --n-bins 5
uv run nasa-virtual-zarr-survey attempt --access external
uv run nasa-virtual-zarr-survey report
```

### Inspecting skipped collections

Collections whose declared format is not array-like (PDF, shapefile, CSV, etc.) are filtered during `discover`. To see the breakdown:

```bash
uv run nasa-virtual-zarr-survey discover --top 50 --skipped
```

### Dry run

To see what would be fetched without writing to the DB:

```bash
uv run nasa-virtual-zarr-survey discover --top 20 --dry-run
```

## Architecture at a glance

```
earthaccess.search_datasets(...)        (Phase 1: discover)
  ↓
stratified temporal sampling            (Phase 2: sample)
  ↓
parse (Phase 3)                         (attempt)
  ├── to_virtual_dataset  (Phase 4a)
  └── to_virtual_datatree (Phase 4b)
  ↓
cubability (Phase 5) + report render
```

State is persisted in a DuckDB database (`output/survey.duckdb`) for checkpoint data, and in DAAC-partitioned Parquet shards (`output/results/`) for the append-only per-attempt log. Both phases are resumable.

For a full design walk-through see the [design document](design/architecture.md).

## License

Distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
