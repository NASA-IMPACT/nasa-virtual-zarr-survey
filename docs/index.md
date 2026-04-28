# virtual-zarr-coverage

Tracks VirtualiZarr coverage of cloud-hosted NASA CMR collections. The pipeline runs as five phases:

1. **Discover** (Phase 1): enumerate CMR collections into `output/state.json`.
2. **Sample** (Phase 2): pick N granules per collection, stratified across positional offsets in CMR's `revision_date` ordering.
3. **Parsability** (Phase 3): the VirtualiZarr parser can produce a `ManifestStore` from a granule URL.
4. **Datasetability / Datatreeability** (Phase 4a / 4b): the `ManifestStore` can be converted to an `xarray.Dataset` (4a) or `xarray.DataTree` (4b). 4b runs in parallel with 4a; it captures hierarchical files that fail 4a with `CONFLICTING_DIM_SIZES`.
5. **Cubability** (Phase 5): the per-granule datasets can be combined into a coherent virtual store. Gated on Phase 4a `all_pass` (tree-only collections are not yet cubable).

Failures in each phase are categorized into an empirically-derived taxonomy so VirtualiZarr maintainers and NASA DAAC operators can prioritize gaps. New to the terms (`granule`, `DAAC`, `ManifestStore`, `cubability`)? See the [glossary](glossary.md). For the bucket meanings, see [the taxonomy reference](design/taxonomy.md).

## Getting started

```bash
uv sync
```

Requires Earthdata Login credentials in `~/.netrc`. See NASA's [Earthdata Login setup guide](https://urs.earthdata.nasa.gov/) if you don't already have an account; the same `.netrc` entry powers both `--access external` (HTTPS + bearer token) and `--access direct` (S3 credentials minted via EDL).

### Run the pipeline

Start small. Run discover, sample, prefetch (for `--access external`), attempt, render on a bounded set of collections so you can review raw errors and refine the failure taxonomy before committing to a full survey:

```bash
uv run vzc discover --top 20
uv run vzc sample --n-bins 2
uv run vzc prefetch         # only needed for --access external
uv run vzc attempt --access external
uv run vzc render
```

`discover` and `sample` are idempotent; `attempt` is resumable (it skips `(collection, granule)` pairs already present in the Parquet log); `render` is cheap and side-effect-free, so you can re-run it after refining the taxonomy without re-fetching anything.

## Run modes in detail

### Scope: `--top` vs `--top-per-provider` vs default

Without a scope flag, `discover` enumerates **all** EOSDIS cloud-hosted collections (thousands). For most iteration that is too much:

- `--top N`: surveys the top-N most-used collections across EOSDIS providers, ranked by CMR's `usage_score`.
- `--top-per-provider N`: takes N per provider, so smaller DAACs are not drowned out by larger ones.

Pick one or the other; the default (no scope flag) is reserved for a full survey.

### Granule depth: `--n-bins`

`--n-bins N` samples N granules per collection, stratified evenly across positional offsets in CMR's `revision_date` ordering. Default is 5. Use 2 for fast iteration; raise it to surface flakiness or long-tail per-granule heterogeneity (which shows up as `partial_pass` verdicts).

### Access mode: `--access external` vs `--access direct`

NASA's EOSDIS S3 buckets live in `us-west-2` and don't allow public direct-S3 reads from outside that region.

- `--access external` (default for outside AWS): HTTPS URLs signed with an Earthdata Login bearer token. Works from anywhere with an `~/.netrc` EDL entry, but every byte goes through HTTPS rather than the cheaper S3 path. Reads come from the local cache only — `prefetch` is the single writer; missing granules in attempt fail fast.
- `--access direct`: temporary S3 credentials minted via EDL's cloud-auth endpoint. Requires that you're running on `us-west-2` compute. Faster and avoids HTTPS gateway costs, but a `403 Forbidden` (taxonomy bucket `FORBIDDEN`) is the typical failure if you try this from outside the region. Skips the cache entirely.

`sample` records both `s3_url` and `https_url` for every granule, so flipping `--access` between runs is free — no re-sampling needed.

### Cloud OPeNDAP / DMR++ sidecars

`discover` records `collections.has_cloud_opendap` (true when the collection is associated with the cloud-Hyrax UMM-S record); `sample` then writes `granules.dmrpp_granule_url = https_url + ".dmrpp"` for each sampled granule of those collections. The constructed URL is the input `DMRPPParser` reads — useful as a fallback when the underlying HDF5 parse fails.

### Previewing the selection: `--list`

Before locking in a top-N selection (and accumulating snapshots against it), eyeball what you'd be sampling. `--list` adds a per-collection table next to the aggregate counts:

| Value | Output |
|---|---|
| `none` (default) | Aggregate counts only. |
| `skipped` | `(format_declared, skip_reason)` breakdown plus a table of skipped collections. |
| `array` | The array-like collections only — the ones that would feed `sample`. |
| `all` | Both array-like and skipped, with a `skip_reason` column (blank for array-like). |

In `--top` and `--top-per-provider` modes the table is sorted by popularity rank and includes `rank` and `usage_score` columns; in non-top modes those columns are blank. The `opendap` column shows `Y` for collections associated with a cloud-OPeNDAP UMM-S record (where DMR++ sidecars and `DMRPPParser` are usable), blank otherwise.

```bash
# preview the top-50 selection before locking it in
uv run vzc discover --top 50 --list array --dry-run

# audit which collections were filtered out and why
uv run vzc discover --top 50 --list skipped --dry-run

# full picture, with skip_reason populated for the filtered rows
uv run vzc discover --top 50 --list all --dry-run
```

The table includes a plain Earthdata Search URL per row (`https://search.earthdata.nasa.gov/search?q=<concept_id>`); modern terminals auto-linkify it for Cmd/Ctrl-click. For a UMM-JSON dump, use `investigate <concept_id> --mode native` — the generated script prints both the search and CMR concept URLs.

`--list` works with or without `--dry-run`; in persisted mode `output/state.json` is populated as a side effect, useful when the listing confirms the selection.

### Dry run

To see what would be fetched without writing state:

```bash
uv run vzc discover --top 20 --dry-run
```

### Caching granule bytes

For `--access external`, the survey reads from a local cache at `~/.cache/nasa-virtual-zarr-survey/` and never fetches over HTTPS at attempt time. Populate the cache with `prefetch` first:

```bash
uv run vzc prefetch --cache-max-size 50GB
```

`prefetch` walks collections in `popularity_rank` order and downloads each sampled granule's `https_url`. Override the cache location with the `NASA_VZ_SURVEY_CACHE_DIR` environment variable. The cap is checked at collection boundaries, so the collection that crosses it finishes writing all its granules before the run stops. See the contributing guide for cache layout and inspection tips.

## Investigating a failure

After a survey run, the most common next step is investigating one specific failure. The `investigate` subcommand emits a self-contained Python script for any concept ID:

```bash
# script that reproduces the survey's VirtualiZarr code path for a collection
uv run vzc investigate C1996881146-POCLOUD > vz_C1996881146.py

# native-library exploration (h5py, netCDF4, astropy, zarr, tifffile) — useful for
# collections skipped at discover time with no Parquet failure to reproduce
uv run vzc investigate C1214470488-ASF --mode native > native_C1214470488.py

# write directly to a file
uv run vzc investigate G1245678901-ASF --mode native --out probes/G1245678901.py
```

The script reads `output/state.json` if it exists and falls back to one or two CMR calls when the concept ID is absent, so it works against a fresh checkout as well as a populated state. `--mode virtual` (the default) reproduces the parser, dataset, and datatree calls the survey ran; `--mode native` dumps UMM-JSON, both `direct` and `external` data links, and (when format can be sniffed) a structural dump via the format-appropriate library.

Run the script with `uv run python vz_C1996881146.py`; it doubles as a working starting point for non-debugging virtualization workflows — edit the kwargs and treat it as a runnable seed.

## Per-collection overrides

Many CMR collections that fail under a naive `attempt` would parse cleanly with the right kwargs (`group="science"` for an HDF5 file whose science variables live under a sub-group, `drop_variables=` to skip a single compound-dtype variable, or simply `skip_dataset = true` for a collection whose dimensions can't be flattened). The override mechanism lets you record those fixes once, in `config/collection_overrides.toml`, and have every future `attempt` pick them up:

```toml
[C1996881146-POCLOUD]
parser = { group = "science", drop_variables = ["status_flag"] }
dataset = { loadable_variables = [] }
notes = "Top-level group has no array vars; descend to /science."

[C2208418228-POCLOUD]
skip_dataset = true
notes = "to_virtual_dataset raises ConflictingDimSizes; datatree path works."
```

The `notes` field is required on every non-empty entry, so the file doubles as a "lessons learned" register that's diff-able and PR-reviewable.

After editing the file, the next `attempt` validates it on startup (concept-ID format, allowed sub-keys, kwarg names against each parser's signature, contradictory combinations like `skip_dataset = true` plus `dataset = {...}`) and fails fast on a malformed entry. Pass `--skip-override-validation` if you'd rather catch validation errors per-attempt at runtime.

The full debug loop is: naive `attempt` records a failure, `investigate` to dig in, edit `collection_overrides.toml`, then re-run `attempt`. The next `render` will mark the collection as `override_applied = true`. See [the design doc](design/architecture.md#per-collection-overrides) for the full schema and validation rules.

## Tracking compatibility over time

The pipeline above is one point in time. To track how VirtualiZarr's compatibility against NASA data evolves across releases — and to evaluate unreleased branches against the same fixed sample — the survey supports *snapshots*.

A snapshot is one re-run of `attempt` + `render` against `config/locked_sample.json` (a committed JSON enumeration of (collection, granule) pairs) under a date-pinned dependency stack. Each snapshot writes a `*.summary.json` digest to `docs/results/history/`; `render --history` renders all of them as a [Coverage over time](results/history.md) page with funnel-over-time and bucket-trend charts.

Two flavors:

- **Release** snapshots pin to a single date (`[tool.uv] exclude-newer`).
- **Preview** snapshots pin to a date plus one or more `[tool.uv.sources]` git overrides — used to evaluate unreleased branches.

Typical workflow once the locked sample is set up (see the contributing guide):

```bash
# release: pin pyproject's exclude-newer date, lock, run.
uv lock
uv run vzc run

# preview: add a git override to [tool.uv.sources], lock, run with a label.
uv lock
uv run vzc run --label variable-chunking

# render the history page after committing new digests.
uv run vzc render --history
```

See [Publishing a snapshot](contributing.md#publishing-a-snapshot) in the contributing guide for the end-to-end walk-through, including how to build the locked sample.

## Querying the raw data

The Parquet log at `output/results/` is the canonical per-attempt record (one row per granule per phase). The fastest way to ask questions like "which DAACs hit `UNSUPPORTED_CODEC` most?" is pyarrow plus a Counter:

```bash
uv run python -c "
from collections import Counter
import pyarrow.parquet as pq
from pathlib import Path

counts: Counter = Counter()
for shard in Path('output/results').rglob('*.parquet'):
    t = pq.read_table(shard, columns=['daac', 'parse_success', 'parse_error_type'])
    for daac, ok, et in zip(*(t[c].to_pylist() for c in t.column_names)):
        if not ok and et:
            counts[(daac, et)] += 1
for (daac, et), n in counts.most_common(20):
    print(f'{n:5d}  {daac}  {et}')
"
```

If you'd rather use a SQL engine, install `duckdb` separately and point it at the Parquet glob — the survey itself doesn't ship it.

`output/state.json` carries the discover/sample state (collections, granules, run metadata). The Parquet log carries every per-attempt result. Together they're enough to recompute the report from scratch.

## Architecture

State is persisted in `output/state.json` for the discover/sample checkpoint, and in DAAC-partitioned Parquet shards (`output/results/`) for the append-only per-attempt log. Both stages are resumable: `discover` and `sample` are idempotent, `attempt` skips already-recorded `(collection, granule)` pairs.

For the full design, including the override mechanism, the repro renderer, and per-format inspectors, see the [design document](design/architecture.md).
