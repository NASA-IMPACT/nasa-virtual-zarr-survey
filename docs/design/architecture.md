# Survey design

## Purpose

Measure, for each cloud-hosted NASA CMR collection of an array-like format, how far the stack gets when asked to virtualize it. The pipeline runs in five phases, of which 3, 4a, 4b, and 5 are the substantive measurement points (1 and 2 are setup):

1. **Discover** (Phase 1): enumerate CMR collections into `output/state.json`.
2. **Sample** (Phase 2): pick N granules per collection, stratified across positional offsets in CMR's `revision_date` ordering.
3. **Parsability** (Phase 3): can a VirtualiZarr parser read the file's metadata into a `ManifestStore`?
4. **Datasetability / Datatreeability** (Phase 4a / 4b): can that `ManifestStore` be materialized into an `xarray.Dataset` (4a) or `xarray.DataTree` (4b)? 4b runs in parallel with 4a so hierarchical files that fail 4a with `CONFLICTING_DIM_SIZES` are still rescued.
5. **Cubability** (Phase 5): across N stratified granules of a collection, are the per-granule datasets structurally compatible enough to be concatenated into one coherent virtual cube?

The same numbering is used in [the user-facing usage docs](../index.md), in the [report](../results/index.md), and in API page headers; treat that listing as the canonical reference. The output is a per-collection verdict table plus a Markdown report that VirtualiZarr maintainers and NASA DAAC operators can act on.

## Scope

**In:** CMR collections with `cloud_hosted=True` hosted by EOSDIS DAAC providers, whose declared or probed format is array-like (NetCDF, HDF, Zarr, GeoTIFF, FITS, DMR++).

**Out:** on-prem-only collections; non-array formats (PDF, shapefile, CSV, binary); non-NASA providers.

**Explicit non-goals:**

- No data read tests. The survey does not call `.compute()` or `.load()`; only metadata / manifest-level opens are exercised.
- No performance benchmarking. `duration_s` is recorded only to surface hangs and pathologically slow opens, not to rank parsers.
- No retries. Flakiness is surfaced as `partial_pass` across the 5 stratified granules rather than hidden.
- No DataTree-aware Cubability. Phase 4b (Datatreeability) is implemented and records whether `ManifestStore.to_virtual_datatree()` succeeds, but the Cubability check (Phase 5) still operates on flat `xr.Dataset` fingerprints; tree-only collections are reported as `NOT_ATTEMPTED` for Cubability.

## Pipeline

Single CLI `vzc` with one subcommand per phase plus `prefetch` (cache writer) and `run` (provenance bundling). Phases share state through a `state.json` checkpoint and a DAAC-partitioned Parquet dataset.

```
earthaccess.search_datasets(cloud_hosted=True, provider=<EOSDIS>)
  ↓
discover  → collections (state.json)                  [Phase 1]
  ↓
sample    → granules   (state.json)                   [Phase 2]
  ↓
prefetch  → on-disk cache (HTTPS-only, popularity order)  [Phase 2.5; required for --access external]
  ↓
attempt   → results.parquet, one row per granule      [Phases 3, 4a, 4b]
  ├─ Parsability:      parser(url=url, registry=...)
  ├─ Datasetability:   manifest_store.to_virtual_dataset()      (4a)
  └─ Datatreeability:  manifest_store.to_virtual_datatree()     (4b, parallel with 4a)
  ↓
render    → docs/results/index.md + figures           [Phase 5: cubability rollup]
```

- `discover` and `sample` are idempotent and safe to re-run.
- `attempt` is resumable: it skips `(collection, granule)` pairs already present in the Parquet log.
- `render` is cheap and side-effect-free; re-run after any taxonomy refinement.

`run` (the snapshot orchestrator) chains `attempt` + `render --no-render --export ...` against `config/locked_sample.json` instead of the live `output/state.json`, writing a `*.summary.json` digest under `docs/results/history/<slug>.summary.json`.

## Package layout

The package is split into five tiers plus three top-level modules. Each tier may import only from layers below it:

```
src/vzc/
├── __init__.py             # public API: 11 re-exported names
├── __main__.py             # click group + register loop
├── _config.py              # hardcoded paths + cache_dir() env lookup
├── snapshot.py             # public `run` (snapshot orchestrator) + RunInputs
│
├── core/                   # data shapes + tiny classifiers
│   ├── types.py            # PendingGranule, VerdictRow, Fingerprint, ...
│   ├── formats.py          # FormatFamily + classify_format
│   ├── taxonomy.py         # Bucket + classify
│   └── processing_level.py # parse_rank
│
├── state/                  # state.json + Parquet readers
│   ├── _io.py              # SurveyState, CollectionRow, GranuleRow, load/save_state
│   ├── _digest.py          # *.summary.json read/write
│   └── _results.py         # pyarrow helpers over results/*.parquet
│
├── cmr/                    # NASA CMR access
│   ├── _discover.py        # fetch_collection_dicts, discover()
│   ├── _sample.py          # sample(), sample_one_collection
│   ├── _popularity.py      # usage_score-ordered top-N picks
│   ├── _opendap.py         # cloud-OPeNDAP UMM-S detection
│   └── _providers.py       # EOSDIS provider list
│
├── pipeline/               # survey compute + diagnostics
│   ├── _attempt.py         # attempt_one + attempt() + AttemptResult
│   ├── _prefetch.py        # HTTPS-only download loop; cache writer
│   ├── _cubability.py      # Phase 5 algorithm + fingerprints
│   ├── _overrides.py       # CollectionOverride + OverrideRegistry
│   ├── _investigate.py     # `investigate(...)`: virtual + native script gen
│   ├── _probe.py           # target resolution + native-mode template
│   ├── _scripts.py         # shared script-template emitters
│   ├── _inspect.py         # per-format structural dispatch (used at runtime)
│   └── _stores.py          # S3 + HTTPS + cache stores; EDL credential cache
│
├── render/                 # markdown + figures + history
│   ├── _orchestrate.py     # public `render()` + private `_run_render()`
│   ├── _aggregate.py       # pyarrow rollups (verdicts, taxonomy, cubability)
│   ├── _markdown.py        # pure renderers: data → Markdown
│   ├── _figures.py         # holoviews + bokeh + matplotlib charts
│   ├── _history.py         # Coverage-over-time page
│   └── _intros.py          # config/feature_introductions.toml reader
│
└── cli/                    # click commands
    ├── _options.py         # --cache-max-size parser
    ├── _summaries.py       # one-line summaries printed by subcommands
    ├── _listings.py        # `discover --list` table renderer
    └── commands/
        ├── discover.py  sample.py     prefetch.py
        ├── attempt.py   render.py     investigate.py
        └── run.py
```

Layer rules (each layer may import only from layers below):

```
cli       → snapshot, render, pipeline, cmr, state, core, _config
snapshot  → render, pipeline, cmr, state, core, _config
render    → state, core, _config
pipeline  → cmr, state, core, _config
cmr       → state, core, _config
state     → core, _config
core      → (stdlib + pyarrow only)
_config   → (stdlib only)
```

Submodules under each tier are underscore-prefixed; the tier's public API lives in `__init__.py` re-exports. Consumers should import from the package, not the underscore submodule (`from vzc.state import load_state`, not `from vzc.state._io import load_state`).

## Measurement model (the phases)

Each phase is recorded independently on every per-granule attempt so reviewers can see where in the stack a failure occurs without re-running the pipeline. Phases 4a and 4b run in parallel after Phase 3 succeeds: a collection can be datasetable, datatreeable, both, or neither.

### Phase 3 — Parsability

```python
parser = dispatch_parser(family)   # HDFParser / NetCDF3Parser / ZarrParser / FITSParser / DMRPPParser / VirtualTIFF
manifest_store = parser(url=url, registry=registry)
```

Success = the parser returned a `ManifestStore` without raising.

Parser dispatch (in `attempt.dispatch_parser`):

| Family | Parser |
|---|---|
| `NETCDF4`, `HDF5` | `virtualizarr.parsers.hdf.HDFParser` |
| `NETCDF3` | `virtualizarr.parsers.netcdf3.NetCDF3Parser` |
| `DMRPP` | `virtualizarr.parsers.dmrpp.DMRPPParser` |
| `FITS` | `virtualizarr.parsers.fits.FITSParser` |
| `ZARR` | `virtualizarr.parsers.zarr.ZarrParser` |
| `GEOTIFF` | `virtual_tiff.VirtualTIFF` (out-of-tree) |
| `HDF4` | *(no parser — recorded as `NoParserAvailable`)* |

### Phase 4 — Datasetability

```python
ds = manifest_store.to_virtual_dataset()
```

Only attempted if Phase 3 succeeded. Exercises xarray's `open_dataset` path wired to VirtualiZarr's manifest store. Captures failures that only surface once `xarray` tries to flatten / coordinate-align the manifest contents (e.g. `conflicting sizes for dimension`, group-structure mismatches).

Recording rule:

- If Phase 3 fails, Phase 4's `dataset_success` is left `NULL` ("not attempted") — **not** `False`. This keeps the parsability signal from being diluted by cascading nulls in downstream aggregates.
- If Phase 4 fails, it is attributed to the Dataset phase even when the underlying cause is arguably upstream (e.g. a parser that eagerly materialized malformed coords). Reviewers should treat the error message as the ground truth, not the phase label.

On Phase 4 success, `extract_fingerprint(ds)` captures a JSON summary of the dataset's structure (see [Fingerprints](#fingerprints)). Failure of fingerprint extraction is swallowed — the attempt is still counted as success.

### Phase 4b — Datatreeability

```python
dt = manifest_store.to_virtual_datatree()
```

Attempted whenever Phase 3 succeeds, **in parallel with Phase 4a (Datasetability)**. A single daemon worker runs parse → dataset → datatree sequentially; three `threading.Event`s let the main thread record per-phase timeouts independently. A failure in 4a does not prevent 4b from running.

Primary motivation: many hierarchical NetCDF4 / HDF5 collections fail `to_virtual_dataset()` with `conflicting sizes for dimension ...` (xarray refusing to flatten nested groups) but succeed as `xr.DataTree`. Capturing 4b separately lets reviewers distinguish "genuinely unreadable" from "flat-only readers get stuck, tree readers succeed."

Recording rule:

- If Phase 3 fails, Phase 4b's `datatree_success` is left `NULL` (same policy as 4a).
- `result.success` is `True` when `parse_success AND (dataset_success OR datatree_success)`, so tree-only successes are still counted as successes.

No fingerprint is captured from the datatree (see Phase 5).

### Phase 5 — Cubability

Run at report time, not at attempt time. Gated on Phase 4a only — collections whose sampled granules all produced an `xr.Dataset`. Tree-only collections (4a failed, 4b succeeded) get `NOT_ATTEMPTED`; extending Cubability to `xr.DataTree` nodes is a future work item.

Collections below `processing_level.CUBE_MIN_RANK` (L3 by default) are short-circuited to `EXCLUDED_BY_POLICY` before the check runs and are removed from the cubability denominator in the rolled-up tables. L2 swath/orbital products are not expected to combine into a single cube, so counting them as cubability failures would be misleading.

For each eligible collection, `check_cubability(fingerprints)` runs a sequence of pass/fail checks:

1. Variable name sets match across granules.
2. Per-variable dtype / dims / codecs match.
3. A concat dimension can be unambiguously identified (preferring a size-varying dim; falling back to a dim with differing coord value hashes).
4. All non-concat dim sizes match.
5. All non-concat coord value hashes match.
6. Per-variable chunk sizes on non-concat axes match.
7. Concat-dim coord ranges are monotonic and non-overlapping across granules.

Verdicts: `FEASIBLE`, `INCOMPATIBLE`, `INCONCLUSIVE` (e.g. ambiguous concat dim, all granules identical), `NOT_ATTEMPTED` (Phase 4 didn't fully pass), `EXCLUDED_BY_POLICY` (processing level below `CUBE_MIN_RANK`).

### Per-granule result record

Every `attempt_one` call produces exactly one `AttemptResult`, serialized as one Parquet row. Key fields:

| Field | Meaning |
|---|---|
| `parse_success`, `parse_error_{type,message,traceback}` | Phase 3 outcome |
| `parse_duration_s` | Wall time inside `parser(...)` only |
| `dataset_success` (nullable), `dataset_error_{type,message,traceback}` | Phase 4a outcome; `NULL` when Phase 3 failed |
| `dataset_duration_s` | Wall time inside `to_virtual_dataset()` only |
| `datatree_success` (nullable), `datatree_error_{type,message,traceback}` | Phase 4b outcome; `NULL` when Phase 3 failed |
| `datatree_duration_s` | Wall time inside `to_virtual_datatree()` only |
| `success` | `parse_success AND (dataset_success OR datatree_success)` |
| `timed_out`, `timed_out_phase` | Per-phase timeout (`parse` / `dataset` / `datatree`); the main thread waits on each phase's event with its own budget |
| `fingerprint` | JSON string, populated only when Phase 4a succeeded; consumed by the cubability check |

## CMR interactions

### Provider universe

`providers.py::get_eosdis_providers()` returns a hard-coded snapshot of EOSDIS DAAC providers, ported from `titiler-cmr-compatibility`. It is a pure list; re-check annually against `https://cmr.earthdata.nasa.gov/search/providers`.

### Discover modes

`discover` has three mutually-exclusive modes:

- **Default:** `earthaccess.search_datasets(cloud_hosted=True, provider=<EOSDIS list>, count=limit)`. `limit=None` means all.
- **`--top N`:** top-N collections across all EOSDIS providers, ranked by CMR's `usage_score`.
- **`--top-per-provider N`:** top-N per provider, concatenated.

Popularity ranking (`popularity.py`) queries `POST https://cmr.earthdata.nasa.gov/search/collections.json` directly — not via `earthaccess` — because the Python wrapper doesn't expose `sort_key[]=-usage_score`. CMR caps any single page at 2000 rows and does not support paging for this sort, so `num > 2000` raises.

For top-N modes, the flow is: fetch concept IDs with `usage_score` sort, then batch-fetch UMM-JSON in chunks of 100 via `earthaccess.search_datasets(concept_id=batch)`.

### UMM-JSON extraction

Per collection (`discover.collection_row_from_umm`):

- **`format_declared`** — prefer `umm.ArchiveAndDistributionInformation.FileDistributionInformation[*].Format` (the actually distributed format), falling back to `FileArchiveInformation[*].Format` (format as archived).
- **`daac`** — first `umm.DataCenters[].ShortName`, falling back to `meta.provider-id`.
- **Temporal extent** — first `umm.TemporalExtents[*].RangeDateTimes[*].{Beginning,Ending}DateTime`. Single-range extents only (we don't walk gaps in discontinuous series).
- **`processing_level`** — `umm.ProcessingLevel.Id`.

Per granule (`sample._extract_*`):

- **`data_url`** — first entry from `DataGranule.data_links(access=<mode>)`. See [Access modes](#access-modes).
- **`size_bytes`** — first `SizeInBytes` (or legacy `Size`) in `umm.DataGranule.ArchiveAndDistributionInformation`.
- **Format (probe)** — when a collection has no collection-level format declared, `sample` calls `earthaccess.search_data(concept_id=..., count=1)` and reads `umm.DataGranule.ArchiveAndDistributionInformation.Format` to reclassify before sampling in earnest.

### Format classification

`formats.classify_format(declared, url)` maps a CMR-declared format string (case-insensitive) or file extension to one of eight `FormatFamily` values. The declared-string mapping is the same one used by `titiler-cmr-compatibility`, extended with `DMR++` and variants.

Collections with no declared format get `skip_reason="format_unknown"` at discover time. `sample` later probes one granule; if the granule's UMM-JSON also lacks a format, or the probed format is non-array, the collection stays skipped. This two-phase probing avoids burning granule queries on thousands of collections at discover time.

Collections whose declared format is known but non-array (shapefile, CSV, PDF, etc.) get `skip_reason="non_array_format"` immediately.

`processing_level` is recorded on every collection but does not gate sampling. Per-granule virtualization (parsability/datasetability) is processing-level-agnostic — an L1B HDF5 granule loads as an `xarray.Dataset` just fine, even if its swath/orbital geometry means it can't combine into a single cube. The L<3 cube-exclusion lives in the cubability phase (`processing_level.CUBE_MIN_RANK`), which is the only phase where the constraint actually applies.

### Sampling

For each array-like collection, 5 granules are sampled using a single positional-stratification path.

Granules are sorted by `revision_date` ascending (CMR sort key). `n_bins` evenly-spaced positional offsets are computed across `coll["num_granules"]`; if that count is `None`, `_hits()` issues one CMR request reading the `cmr-hits` response header to refresh it. For each offset, `_fetch_at_offset(concept_id, offset)` calls `cmr.earthdata.nasa.gov/search/granules.umm_json?collection_concept_id=...&sort_key=revision_date&page_size=1&page_num=offset+1`. If a bin returns no granule (rare CMR pagination race), the request is retried once at the adjacent offset (`offset - 1`, or `offset + 1` when `offset == 0`); on second failure a warning is logged and the bin is skipped. When `num_granules <= n_bins`, all granules are fetched directly rather than stratifying.

Sorting by `revision_date` rather than observation time means codec heterogeneity introduced by reprocessing campaigns lands in different bins by construction: a 2010 swath reprocessed in 2025 carries the 2025 codec, and positional stratification captures it. CMR stamps `revision_date` on every granule at ingest, so this strategy works equally well for collections with or without a declared temporal extent.

Each granule row records both `s3_url` (for `--access direct`) and `https_url` (for `--access external`). Sample makes one `data_links` call per access mode at sample time, so flipping `--access` on later runs requires no re-sampling — `attempt` just picks the matching URL off the granule.

### Rate-limiting and politeness

No explicit rate limiting. `discover` issues O(1) CMR calls (one paged `search_datasets`, or N/100 concept-id batches in top-N mode). `sample` issues O(collections × 5) granule-search calls. `attempt` hits S3 / DAAC HTTPS gateways directly and does not touch CMR. A full survey is ~10k attempts and runs in ~1 workday.

## VirtualiZarr interactions

### Why two phases rather than `open_virtual_dataset`

`virtualizarr.open_virtual_dataset(url, registry, parser)` wraps both phases in one call. We split them because the survey's value is discriminating failures. Without the split, a single error column cannot tell a reviewer whether to file a parser issue or an xarray / manifest-store issue.

Concretely, inside `attempt.attempt_one`:

```python
ms = parser(url=url, registry=registry)      # Phase 3
ds = ms.to_virtual_dataset()                 # Phase 4
```

### Registry construction

```python
# _build_registry
parsed = urlparse(url)
scheme = parsed.scheme or "s3"
bucket = parsed.netloc
return ObjectStoreRegistry({f"{scheme}://{bucket}": store})
```

One registry entry per attempt, keyed by `scheme://netloc`. In direct mode the netloc is an S3 bucket; in external mode it is a DAAC HTTPS host. This works because every URL for a given attempt lives under one host / bucket; VirtualiZarr parsers that follow internal references (e.g. DMR++ pointing to a sidecar) within the same host / bucket are naturally covered, but cross-host references are not.

### Timeout mechanism

Each attempt runs inside a daemon `threading.Thread`. The worker fires three `threading.Event`s — one per phase (parse, dataset, datatree) — as each completes. The main thread waits on each event in turn with its own `timeout_s` budget:

```python
thread = threading.Thread(target=_runner, daemon=True)
thread.start()
for phase_name, event in [("parse", parse_done), ("dataset", dataset_done), ("datatree", datatree_done)]:
    if not event.wait(timeout=timeout_s):
        result.timed_out = True
        result.timed_out_phase = phase_name
        break
```

If any phase's event doesn't fire in time, the main thread records the timeout, stops waiting, and abandons the worker. Subsequent events may still fire in the background; their results are not captured. The daemon flag ensures Python exits at process end even if the worker is still blocked on I/O. The per-attempt thread leak is acceptable because the pipeline is sequential and parser instances are fresh on every attempt.

Budget note: because each phase gets its own `timeout_s` budget, the worst-case wall time for a fully-hanging attempt is `3 × timeout_s`. That's a deliberate choice over a shared countdown — it keeps per-phase diagnosis unambiguous and gives each phase a full budget regardless of what came before.

### Parser instantiation

A fresh parser instance per attempt (`dispatch_parser` always returns `X()`, never a cached instance). This avoids parsers accumulating internal state between granules of different collections and keeps attempts independent.

### Fingerprints

On Phase 4 success, `cubability.extract_fingerprint(ds)` walks the resulting xarray `Dataset` metadata — no data values — and writes a JSON object with:

- `dims: {name: size}`
- `data_vars: {name: {dtype, dims, chunks, fill_value, codecs}}` where `codecs` is derived from `var.encoding.{compressor, filters, codecs}` (type names only).
- `coords: {name: {dtype, dims, shape, values_hash, min, max}}` — coord values are hashed (`sha256(arr.tobytes())`) and reduced to sorted-endpoints so inter-granule compatibility can be checked without paying the round-trip cost.

Reviewers should weigh in on whether this is a faithful enough summary for the cubability decisions that depend on it. In particular: chunk shape from `var.chunks[0]` per dim assumes uniform chunking; `codecs` by type name loses parameterization.

### What's not exercised

- `.compute()` / data reads — only metadata-level opens.
- Writing (e.g. to Icechunk). The survey stops at an in-memory virtual dataset.
- Concat across granules. Cubability is a *feasibility check*, not an actual combine — no `xr.concat` is ever run.

#### Read-validation gap

Phase 4a/4b record `dataset_success = True` once `ManifestStore.to_virtual_dataset(...)` returns an `xarray.Dataset` — no chunks are fetched through the manifest. Cubability (Phase 5) calls `.values` on coordinate variables only; data variables are never read.

Failure modes this leaves uncaught:

- Manifest builds with chunk records pointing at wrong byte offsets or lengths — reads return garbage with no construction-time error.
- Manifest carries wrong codec metadata (e.g. mistaken filter pipeline, wrong endianness) — chunks decode to wrong values.
- Fill-value, scale-factor, or add-offset misinterpretation between the parser and the source file's encoding.

Reads through the virtual store are tested elsewhere — VirtualiZarr's own per-format test suite — so this survey trusts construction success as a proxy for readability. That trust is the right tradeoff while the survey's job is breadth (parser coverage across NASA), not re-litigating per-format correctness. It becomes the wrong tradeoff if the survey's outputs are cited as "VirtualiZarr supports N% of NASA collections" without qualification.

Two designed avenues to close the gap, in increasing thoroughness and cost:

1. **Slice-and-compare per granule.** Pick one numeric data variable, read its first chunk through the virtual dataset, and read the same slice directly from the source file (h5py for HDF5/NetCDF4, format-specific readers elsewhere). Compare with `numpy.array_equal` / `numpy.allclose`. Cost: roughly one extra chunk fetch per attempt. Catches: wrong-bytes, wrong-codec, wrong-fill in chunk #0. Misses: failures that only manifest in later chunks or in non-sampled variables.
2. **Cache + full xarray-native compare.** With `--access external` (where the granule is already cached locally), open the cached file with `xr.open_dataset(...)` (or rioxarray / astropy where needed) using matched decode kwargs (`decode_cf=False, mask_and_scale=False`) and compare against the virtual dataset with `xr.testing.assert_allclose(..., equal_nan=True)`. Cost: full file load on both sides per attempt; potentially 10–100× slower. Catches: structural drift, wrong-bytes anywhere, missing variables, dtype/dim mismatches. Risk: false positives from attribute drift between backends — a curated ignore list is needed.

Recommendation when this work is picked up: start with avenue 2 gated on `--access external` (cache present), record outcomes in new `read_*` columns on the per-granule Parquet log, and surface the result as a fourth funnel line in the report. Avenue 1 is a fallback only if avenue 2's runtime proves untenable.

## Access modes

Two modes are supported end-to-end. `--access` plumbs through `sample` (which URL to store) and `attempt` (which store type to build). The rest of the pipeline is mode-agnostic because the registry keys on `scheme://netloc`.

### `direct` (default)

- Granule URLs are S3 (`s3://bucket/key`), from `DataGranule.data_links(access="direct")`.
- Requires AWS compute in `us-west-2` (NASA S3 direct access is region-locked).
- Store: `obstore.store.S3Store`, one per `(provider, bucket)` pair.
- Credentials are minted per provider via `earthaccess.get_s3_credentials(provider=...)` and cached with a 50-minute TTL (EDL expires them at 60 minutes). On TTL expiry, all cached stores for the provider are invalidated before a new credential is minted.
- **Forbidden abort:** if five consecutive attempts in direct mode classify as `Bucket.FORBIDDEN` (403 / AccessDenied), the run aborts with a message suggesting `--access external` and reminding the user that sampled URLs differ between modes and must be regenerated. This catches the common "running outside us-west-2" footgun fast.

### `external`

- Granule URLs are HTTPS (`https://<daac-host>/path/file.nc`), from `DataGranule.data_links(access="external")`.
- Works from anywhere with EDL credentials in `~/.netrc`.
- Store: `obstore.store.HTTPStore.from_url(f"{scheme}://{host}", default_headers={"Authorization": f"Bearer {edl_token}"})`, cached per `scheme://host`.
- Token source: after `earthaccess.login(strategy="netrc")`, we pull `earthaccess.__auth__.token["access_token"]`.
- Many DAAC HTTPS gateways 302-redirect to presigned S3 URLs. This is expected; `HTTPStore`'s default redirect behaviour handles it. See [Known Limitations](#known-limitations) for the caveat about bearer-header retention across redirects.

Both stores raise `AuthUnavailable` on missing credentials / tokens; the caller records the failure as a per-granule `AuthUnavailable` result rather than aborting.

## Local granule cache

Re-running `attempt` after a VirtualiZarr bump, a taxonomy refinement, or a new override means re-fetching bytes for every sampled granule. The same applies to `investigate` iteration on a single granule's parser kwargs. Most failures the survey records are properties of the granule (parser bug, malformed file, unsupported codec) rather than the network path, so reading from a local cache between iterations does not mask the signal.

The cache regime is split between writer and reader by access mode:

- `prefetch` is the **only** writer of the on-disk cache. HTTPS-only — it walks `state.collections` in `popularity_rank` order and downloads each sampled granule's `https_url`. Cache dir defaults to `~/.cache/nasa-virtual-zarr-survey/`, override via `NASA_VZ_SURVEY_CACHE_DIR`.
- `attempt --access external` is **cache-only**: it reads from the cache and fails fast on miss. The operator runs `prefetch` first; missing granules don't fall through to a network fetch.
- `attempt --access direct` skips the cache entirely. In-region S3 reads are fast and free, so caching adds no value.

`pipeline/_stores.py` exposes `download_url_to_cache` (used by `prefetch`) and `cache_layout_path` (used by `attempt --access external`). Files are persisted to `<cache_dir>/<scheme>/<host>/<sha256(url)>`.

### Cache key

`sha256(url)` against the granule's `https_url`. Stable for the survey's purposes: granule URLs don't change for a given concept ID across runs.

### Cap

`--cache-max-size SIZE` on `prefetch` (default `50GB`). The cap is enforced at **collection boundaries**: the collection that pushes total cache size past the cap finishes writing all its granules before prefetch stops. No mid-collection truncation; no eviction.

### Failure modes

- **Cache dir not writable** (read-only FS, permission error on first write): prefetch logs once and exits with an error.
- **Disk full mid-write**: catch `OSError`, delete the partial `*.tmp`, log warning, move on to the next granule.
- **Per-granule fetch failure**: logged with the error type and message; siblings in the same collection are still attempted.
- **Stale `.tmp`** from a crashed prior run: ignored on read, overwritten on next write.

### Caveats

- *Sequential safety only.* Atomic rename plus in-process accounting is sufficient for single-process runs and occasional worktree overlap, not a future ProcessPool.
- *attempt --access external is cache-only.* If you forget to run `prefetch` before `attempt`, every granule fails fast with a cache-miss error. Re-run `prefetch` (or the failing collection alone with `prefetch --collection`) and retry.

### Non-goals

- Range-level caching. Whole-granule only; a parser that reads 5 MB of a 1 GB file still pulls the full GB into the cache.
- Negative caching. Failed responses (403, 404, timeouts) are never written.
- Auto-eviction. Append-only; the operator clears the cache when it grows too large.
- Caching at non-granule layers (CMR JSON responses, EDL credentials). Cheap relative to granule fetches and out of scope.

## Per-collection overrides

`attempt.py` constructs every parser with default arguments and calls `manifest_store.to_virtual_dataset()` and `to_virtual_datatree()` with no kwargs. Many CMR collections that fail under that naive call shape would parse cleanly with the right kwargs — `group=` for an HDF5 file whose science variables live under a sub-group, `drop_variables=` to skip a single variable with a compound dtype, or `skip_dataset = true` for a collection whose dimension structure cannot be flattened. Without an override layer, the survey re-records the same failure on every run and the institutional knowledge ("collection C123 needs `group='science'`") lives only in operator memory.

The override mechanism lets an operator run the survey naively, debug a failure with `repro`, and record the fix in a checked-in artifact that future `attempt` runs pick up automatically. The artifact is diff-able and PR-reviewable so it doubles as the survey's "lessons learned" register.

### Configuration file

`config/collection_overrides.toml`, checked in. Each top-level table is keyed by CMR collection concept ID:

```toml
[C1996881146-POCLOUD]
parser = { group = "science", drop_variables = ["status_flag"] }
dataset = { loadable_variables = [] }
notes = "Top-level group has no array vars; descend to /science."

[C2208418228-POCLOUD]
skip_dataset = true
notes = "to_virtual_dataset raises ConflictingDimSizes; datatree path works."

[C2746966926-LPCLOUD]
skip_dataset = true
skip_datatree = true
notes = "Tracked: https://github.com/zarr-developers/VirtualiZarr/issues/1234"
```

### Validation rules at load time

Loaded eagerly at the top of `attempt`, so a malformed entry fails the run before any granule is touched.

1. Top-level keys match `^C\d+-[A-Z]+$`.
2. Per-collection sub-keys limited to: `parser`, `dataset`, `datatree`, `skip_dataset`, `skip_datatree`, `notes`. Anything else rejected.
3. `parser` / `dataset` / `datatree` values must be inline tables (or absent).
4. `parser` kwarg names checked against the dispatched parser's `__init__` signature for that collection's format family. A `group=` on a `NoParserAvailable` family is an error.
5. `dataset` kwargs validated against `ManifestStore.to_virtual_dataset`'s signature; `datatree` against `to_virtual_datatree`'s.
6. `skip_dataset = true` combined with `dataset = {...}` is an error (contradictory). Same for datatree.
7. `notes` is required on any non-empty entry — a one-line rationale for review.

Validation runs at the start of every `attempt`; pass `--skip-override-validation` to defer it to per-attempt runtime instead.

### Module layout

`overrides.py` is the only new code surface for the mechanism:

- `OverrideRegistry.from_toml(path) -> OverrideRegistry` parses and validates.
- `registry.for_collection(concept_id) -> CollectionOverride` returns an immutable record carrying `parser_kwargs`, `dataset_kwargs`, `datatree_kwargs`, `skip_dataset`, `skip_datatree`. A miss returns the empty default.
- `apply_to_parser(parser_cls, kwargs)`, `apply_to_dataset_call(manifest_store, kwargs)`, `apply_to_datatree_call(manifest_store, kwargs)` are used by both `attempt_one` and the repro renderer so application logic lives in one place.

`attempt_one` accepts `override: CollectionOverride | None = None` (defaulting to no-op); `attempt` loads the registry once at startup and passes the matching override into each call. The worker thread and timeout logic are untouched.

### Effect on results

`AttemptResult` and the Parquet schema gain a single new column, `override_applied: bool`, true when any kwarg or skip flag was used. The report distinguishes "succeeded naively" from "succeeded with an override" — different signals to a downstream reader. Override kwargs themselves are *not* serialized into Parquet; the TOML file plus the git SHA at run time are the canonical record.

### Non-goals

- Per-granule overrides. The granularity is collection only.
- A CLI to edit the override file. TOML is short and structured; a CLI for "set a key in a TOML file" is friction.
- Backporting overrides to existing failed result rows. A re-run after editing the file is the supported workflow.

## Investigate (script generation)

`investigate <CONCEPT_ID>` emits a runnable Python script for one CMR collection or granule. It replaces the earlier `repro` (survey-path reproduction) and `probe` (native exploration) commands with a single tool gated by `--mode`. Pipe the output to `uv run python -` to execute now, or `--out PATH` to write a file for later iteration.

### Two modes

- **`--mode virtual`** (default). Emits a script that imports `attempt_one` from the survey package and calls it with the resolved URL + format family. The reader steps through the survey's own VirtualiZarr code path (parser → manifest_store → `to_virtual_dataset` / `to_virtual_datatree`) for stepping through a parser- or xarray-level failure with the same overrides the survey runs under. Matching entries from `config/collection_overrides.toml` are baked into the script automatically.

- **`--mode native`**. Emits an exploration script using the format-appropriate library (`h5py` for HDF5/NetCDF4, `netCDF4` for NetCDF3, `astropy` for FITS, `zarr` for Zarr, `tifffile` for GeoTIFF). Useful for format triage independent of any virtualization, or when investigating a collection skipped at discover time (`skip_reason='format_unknown'`).

### Target resolution

`investigate` prefers the local `output/state.json` and falls back to one or two CMR calls only when the concept ID is absent:

| Input | Local state | CMR calls at gen time |
|---|---|---|
| `G456` | granule in state | 0 |
| `G456` | granule not in state | 1 (`search_data`) |
| `C123` | collection + granules in state | 0 |
| `C123` | collection in state, no granules | 1 (`search_data`) |
| `C123` | collection not in state | 2 (`search_datasets` + `search_data`) |

`investigate` is per-target — each invocation inspects exactly one granule. To look at more, re-run with another concept ID. The lookup logic is shared between modes (see `pipeline/_probe.py`).

### `_inspect.py` — runtime per-format structural dispatch

Native-mode scripts call `pipeline._inspect.inspect_url`, which dispatches per `FormatFamily`:

- **HDF5 / NetCDF4** — `h5py` walk: per group, list datasets with `shape`, `dtype`, `chunks`, `compression`, `compression_opts`, attached dim scales, fillvalue, top 10 attrs.
- **Zarr** — open via `zarr-python`, dump `zarr.json` hierarchy: arrays with shape/dtype/chunks/codecs and group attrs.
- **NetCDF3** — `netCDF4` (or `scipy.io.netcdf`) for dims, vars, and attrs.
- **GeoTIFF / COG** — `tifffile`: IFDs, predictor, compression, tile/strip layout, photometric, GeoKeys.
- **DMR++** — parse the XML and dump shape/dtype/chunk/filter info.
- **FITS** — `astropy.io.fits.info()` plus per-HDU header extract.

Two principles: the inspector reads through the same `obstore` `S3Store` / `HTTPStore` the parser uses (no separate auth path), and output is human-readable text *and* machine-parseable — a JSON blob block at the end of stdout, fenced with `<<<INSPECT_JSON_BEGIN>>>` ... `<<<INSPECT_JSON_END>>>` sentinels for downstream tooling.

### Shared script template

Both modes compose their output from `pipeline/_scripts.py`, which exposes pure string-snippet emitters: `render_cache_argparse`, `render_earthaccess_login`, `render_store` / `render_login_and_store`, `render_cache_wiring`, `render_inspect_block`.

### The debug loop

1. Naive `attempt` records `C123-DAAC` failing with e.g. `CONFLICTING_DIM_SIZES`, `override_applied = false`.
2. `vzc investigate C123-DAAC | uv run python -` runs the survey path against the failing granule. Edit kwargs in the script until something works, or conclude it is an upstream bug.
3. Hand-edit `config/collection_overrides.toml`; add a section for `C123-DAAC` with `notes` linking to the issue or repro filename.
4. Re-run `attempt`. Override validation runs at startup, so a malformed entry fails fast.
5. Existing pending-granule logic skips already-attempted granules, so a re-attempt requires removing that collection's rows from the Parquet log first — the manual operation is `find output/results/ -name '*.parquet' -delete` for a small survey, or surgically removing the affected rows from a single shard with pyarrow.
6. Full re-run records `override_applied = true`.

### Bucket triage

The inspector is sufficient for hand-debugging the High and Medium failure buckets:

| Category | Bucket | Fix kind | Inspector helps? |
|---|---|---|---|
| High | `GROUP_STRUCTURE` | `parser.group=` | Yes — group name is in the dump. |
| High | `CONFLICTING_DIM_SIZES` | `skip_dataset` or deeper `parser.group=` | Yes — group dim shapes visible. |
| Medium | `COMPOUND_DTYPE` / `STRING_DTYPE` | `drop_variables=[offending]` | Yes — dtype column flags the offender. |
| Medium | `UNDEFINED_FILL_VALUE` | `drop_variables=[offending]` | Yes — fillvalue column flags the offender. |
| Medium | `UNSUPPORTED_CODEC` / `UNSUPPORTED_FILTER` | `drop_variables=` workaround OR upstream issue | Partial — inspector names the codec; real fix is upstream. |
| Issue-only | `SHARDING_UNSUPPORTED`, `VARIABLE_CHUNKS`, `NON_STANDARD_HDF5`, `AMBIGUOUS_ARRAY_TRUTH` | Upstream issue | Inspector helps draft the issue body; no kwarg fix exists. |
| Low | `TIMEOUT`, `FORBIDDEN`, `NETWORK_ERROR`, `NO_PARSER`, `CANT_OPEN_FILE`, `SAMPLE_INVALID`, `AUTH_UNAVAILABLE` | Environmental or upstream | Inspector adds nothing. |

## Data model

### State (`output/state.json`)

A single JSON document with three flat lists, defined in `vzc.state` as dataclasses:

```python
@dataclass
class CollectionRow:
    concept_id: str
    short_name: str | None
    version: str | None
    daac: str | None
    provider: str | None
    format_family: str | None      # one of FormatFamily, None if unknown / non-array
    format_declared: str | None    # raw CMR-declared string, for debugging
    num_granules: int | None
    time_start: str | None         # ISO datetime
    time_end: str | None
    processing_level: str | None
    skip_reason: str | None        # None | 'non_array_format' | 'format_unknown'
    has_cloud_opendap: bool        # UMM-S association with cloud Hyrax (DMR++ usable)
    popularity_rank: int | None
    usage_score: int | None
    discovered_at: str | None
    umm_json: dict | None          # full top-level CMR response: {meta, umm}

@dataclass
class GranuleRow:
    collection_concept_id: str
    granule_concept_id: str
    s3_url: str | None             # for --access direct
    https_url: str | None          # for --access external
    dmrpp_granule_url: str | None  # https_url + ".dmrpp" when has_cloud_opendap
    stratification_bin: int
    n_total_at_sample: int
    size_bytes: int | None
    sampled_at: str | None
    umm_json: dict | None
```

Both lists carry a `umm_json` field holding the full top-level CMR response (`{meta, umm}`) for that row. The pipeline only branches on the dedicated fields above; everything else (DOI, EntryTitle, platforms, GranuleSpatialRepresentation, DirectDistributionInformation, RelatedUrls, …) is read out via plain dict access, e.g. `coll.umm_json["umm"]["DOI"]["DOI"]`. UMM revision/version travels inside `meta`, so no separate version field is stored.

Top-level schema:

```jsonc
{
  "schema_version": 1,
  "collections": [ /* CollectionRow ... */ ],
  "granules":    [ /* GranuleRow ... */ ],
  "run_meta":    { "sampling_mode": "top=200" }
}
```

Two URL flavours are stored per granule because flipping `--access` between runs is then free — neither `discover` nor `sample` re-runs are needed. Same shape is committed as `config/locked_sample.json`; locked samples just trim `umm_json` for size.

`SCHEMA_VERSION` is bumped whole on any breaking change. There are no migrations: a mismatch raises `ValueError` and the operator deletes `output/state.json` and re-runs `discover && sample`.

### Parquet (`output/results/DAAC=<daac>/part-NNNN.parquet`)

Append-only per-attempt log. One row per `attempt_one` call. Partitioned by DAAC so partial reprocessing is easy and a crash only risks the active shard's in-memory buffer.

Notable columns (full list in `attempt._SCHEMA_FIELDS`):

| Column | Type | Notes |
|---|---|---|
| `collection_concept_id` | STRING | |
| `granule_concept_id` | STRING | |
| `daac` | STRING | partition key |
| `format_family` | STRING | `FormatFamily.value` |
| `parser` | STRING | `type(parser).__name__` |
| `parse_success` | BOOL | Phase 3 |
| `parse_error_{type,message,traceback}` | STRING | truncated to 2/2/4 KB |
| `parse_duration_s` | DOUBLE | parser call only |
| `dataset_success` | BOOL (nullable) | Phase 4; NULL when Phase 3 failed |
| `dataset_error_{type,message,traceback}` | STRING | |
| `dataset_duration_s` | DOUBLE | `to_virtual_dataset()` only |
| `success` | BOOL | parse AND dataset succeeded |
| `timed_out`, `timed_out_phase` | BOOL, STRING | `"parse"` \| `"dataset"` |
| `duration_s` | DOUBLE | wall time including dispatch |
| `fingerprint` | STRING | JSON; populated only on full success |
| `attempted_at` | TIMESTAMP | UTC |

Shards rotate every 500 rows (`ResultWriter.shard_size`). A SIGINT handler flushes every DAAC's buffered rows before exiting with code 0, so reruns resume cleanly.

### Resume logic

`state.pending_granules(state, results_dir)` joins `state.granules` with `state.collections` and excludes pairs already present in the Parquet log:

```python
done = results.attempted_pairs(results_dir)  # set[(coll_id, gran_id)]
out = [
    g for g in state.granules
    if (coll := state.collection(g.collection_concept_id)) is not None
    and coll.skip_reason is None
    and (g.collection_concept_id, g.granule_concept_id) not in done
]
```

On the first run, `results_dir` has no Parquet shards and `done` is empty — so every granule of every array-like collection is pending. Results are ordered by `(daac, collection, stratification_bin)` so per-collection progress lines to stderr are meaningful and shards stay DAAC-local.

## Reporting

`render/_aggregate.py` reads `output/state.json` (via `vzc.state.load_state`) and the full Parquet log (via `vzc.state.iter_rows`, a thin wrapper over `pyarrow.parquet.read_table` that yields one Python dict per shard row). All aggregation is done in pure Python — no SQL engine. The emitted Markdown contains:

- Overview Sankey (collections → parsable → datasetable → cubable).
- Per-phase verdict tables (`all_pass` / `partial_pass` / `all_fail` / `not_attempted` / `skipped`). `skipped` is assigned at the collection level from `collections.skip_reason`; the other verdicts are derived from the Parquet log.
- Per-phase failure taxonomy (see [Failure Taxonomy](taxonomy.md)) with both granule and distinct-collection counts.
- Per-DAAC and per-format-family tables in the form "parsable / datasetable / cubable (% of the previous column)."
- Top-50 raw errors per phase for the `OTHER` bucket, seeding the next round of taxonomy refinement.
- Full per-collection table at the end.

## Snapshots and history

The reporting pipeline above is a single-point-in-time view: it shows compatibility under whatever versions of VirtualiZarr / xarray / zarr happen to be installed. To track compatibility *over time* across releases (and to evaluate unreleased branches), the survey overlays a snapshot system on top.

### Locked sample

The unit of comparison across snapshots is the **locked sample**: a deterministic JSON file at `config/locked_sample.json` enumerating the (collection, granule) pairs every snapshot is evaluated against. It is committed and regenerated only deliberately. The committed file uses the same shape as `output/state.json` (typically with `umm_json` trimmed for size), so the operator regenerates by copying live state.json and pruning fields.

`run` is the snapshot orchestrator. It loads `config/locked_sample.json` into a `SurveyState` (via `state.load_state`), then drives the private `pipeline._run_attempt(state, ...)` and `render._run_render(state=, results_dir=, ...)` helpers — the same engines the public `attempt()` / `render()` use, but with state injected from the locked sample and results pinned under `output/snapshots/<slug>/results/`.

Each summary digest carries `locked_sample_sha256`; a mismatch across digests is the renderer's signal that two snapshots are not directly comparable, and surfaces as a warning when the Coverage-over-time page runs.

### State loading

There is no `Session` wrapper. State read/write is free functions in `state/_io.py`:

- `load_state(path)` — read a state.json (or locked-sample JSON, same schema) into a `SurveyState`.
- `save_state(state, path)` — write a `SurveyState` back to disk.
- `pending_granules(state, results_dir)` — antijoin against the Parquet log.

Public phase functions take no path arguments — `output/state.json`, `output/results/`, and `docs/results/index.md` are hardcoded relative to cwd. The private `_run_attempt` / `_run_render` helpers (used by `run`) accept explicit state + paths so the snapshot orchestrator can inject a locked-sample state and re-target outputs without monkey-patching cwd.

### Summary digest

`docs/results/history/<slug>.summary.json` is the per-snapshot artifact. It is the `render --export` digest plus snapshot metadata:

| Field                   | Purpose                                                            |
|------------------------|--------------------------------------------------------------------|
| `snapshot_date`        | ISO date — the `[tool.uv] exclude-newer` value, or `--snapshot-date` override. |
| `snapshot_kind`        | `"release"` or `"preview"`.                                        |
| `label`                | Filename-safe slug (preview only).                                 |
| `description`          | One-line note (preview only).                                      |
| `locked_sample_sha256` | SHA-256 of the locked-sample JSON. Used to detect drift.           |

The schema is bumped whole — there are no migrations. Older digests are regenerated from a re-run.

### Release vs preview

A **release snapshot** pins to a date alone (`[tool.uv] exclude-newer`). A **preview snapshot** pins to a date *plus* one or more git-sourced packages in `[tool.uv.sources]` — used to evaluate unreleased work (a VirtualiZarr branch, an xarray PR) against the same locked sample. The operator pins both via `pyproject.toml` and `uv lock` before invoking `run`; the snapshot itself is mode-agnostic (the `--label` flag is what marks it as a preview).

The reproducibility contract is **package version strings + locked-sample SHA-256** — captured in the digest. Recreating the exact resolved env tree is not a supported workflow; if you need byte-exact reproduction, capture `uv.lock` yourself before running.

### History page rendering

`render._history.run_history` reads every `*.summary.json` under `docs/results/history/` and emits `docs/results/history.md` with: a snapshots table, a funnel-over-time chart (% pass per phase per snapshot, with hand-curated feature-introduction markers from `config/feature_introductions.toml`), and a top-N bucket trend chart. The chart toolchain mirrors `render._figures`: holoviews + bokeh for interactive HTML, matplotlib for PNG fallbacks. CLI entry: `vzc render --history`.

Feature annotations are not introspected — they are hand-authored entries in `config/feature_introductions.toml` keyed by feature name with `phases`, `first_in_vz`, `introduced` (date), and `description`. Empty file is the valid initial state.

## Error handling

- **Per-attempt:** all exceptions caught; errors serialized into the Parquet row. Timeouts → `TimeoutError`. Auth failures → `AuthUnavailable`.
- **No retries.** Flakiness surfaces naturally as `partial_pass` across the 5 stratified granules. Retrying inside `attempt_one` would conflate transient failure with genuine lack of support and muddy the taxonomy.
- **Process-level SIGINT:** flushes every DAAC's active shard, then `sys.exit(0)`. The next `attempt` run resumes from where it stopped.
- **Forbidden run-abort:** 5 consecutive direct-mode 403s cause a clean abort with a remediation message. Non-direct runs do not trigger this.

## CLI

Click-based. Eight subcommands. Paths are hardcoded relative to the current working directory — there are no `--db`, `--state`, `--results`, or `--out` flags on the phase subcommands. Tests use `monkeypatch.chdir(tmp_path)` to redirect.

- **Pipeline:** `version`, `discover`, `sample`, `prefetch` (HTTPS-only cache writer; required before `attempt --access external`), `attempt`, `render`.
- **Diagnostic:** `investigate ID --mode {virtual|native}` — emits a runnable Python script for one concept ID. Replaces the older `probe` (native exploration) and `repro` (survey-path reproduction) commands.
- **Snapshot orchestrator:** `run` runs `attempt` + `render --no-render --export …` against `config/locked_sample.json` and writes a `*.summary.json` digest under `docs/results/history/<slug>.summary.json`. Reads `[tool.uv] exclude-newer` from `pyproject.toml` for the default date.

Discovery-specific (`discover`):

- `--limit N`, `--top N`, `--top-per-provider N` (mutually exclusive scope flags)
- `--dry-run`, `--list {none,skipped,array,all}` (default `none`)

Sample-specific (`sample`):

- `--n-bins N` (default 5)

Prefetch-specific (`prefetch`):

- `--cache-max-size SIZE` — default `50GB`, accepts human-readable strings (`50GB`, `2.5TB`)
- `--max-granule-size SIZE` — skip whole collections containing any granule larger than this
- `--collection ID` — restrict to one collection (bypasses the popularity-rank requirement)
- `-v, --verbose` — per-granule progress lines
- `NASA_VZ_SURVEY_CACHE_DIR` env var — default `~/.cache/nasa-virtual-zarr-survey/`

Attempt-specific (`attempt`):

- `--access {direct,external}` (default `direct`). `external` is cache-only and requires `prefetch` to have populated the cache; `direct` skips the cache entirely.
- `--timeout SECONDS` (default 60)

Render-specific (`render`):

- `--from-data PATH` — regenerate from a committed `*.summary.json` digest, skip state / Parquet queries
- `--history` — also re-render the Coverage-over-time page

Investigate-specific (`investigate`):

- positional `CONCEPT_ID` — `C…` or `G…`, auto-detected by prefix
- `--mode {virtual,native}` (default `virtual`)
- `--access {direct,external}` (default `external`) — selects the URL flavour the script binds to
- `--out PATH` — write the script to this file; default stdout

Run-specific (`run`):

- `--snapshot-date ISO_DATE` — default from `[tool.uv] exclude-newer`
- `--label LABEL` — marks the run as a preview
- `--description TEXT` — one-line note (only meaningful with `--label`)
- `--access {direct,external}` (default `external`)

`__main__.py` suppresses noisy upstream warnings (`earthaccess` `DataGranule.size` `FutureWarning`, Numcodecs / Imagecodecs "not in Zarr v3 spec" `UserWarning`s, etc.) to keep stderr meaningful.

## Testing

### Unit (`tests/unit/`)

Every module has mocked tests for its public API. Notable suites:

- `test_taxonomy.py` — table-driven, one case per hypothesized bucket; grows as new error patterns surface.
- `test_auth.py` — store dispatch mocked at `earthaccess`, `obstore.store.S3Store`, and `HTTPStore`. Does not exercise real S3 or real EDL. Covers `StoreCache.get_store` returning S3Store for `direct` mode, `ReadOnlyCacheStore` for `external` mode.
- `test_cache.py` — cache layout (`cache_layout_path`), size accounting (`CacheSizeTracker.would_exceed`), and the read-only cache store: hit serves from disk; miss raises `FileNotFoundError`; `download_url_to_cache` writes atomically under `*.tmp` rename.
- `test_attempt.py::test_run_attempt_resumes` — pre-populates a Parquet shard, verifies the resume check skips already-attempted granules.
- `test_attempt.py` (override path) — `override_applied` is recorded correctly when an override is matched.
- `test_sample.py` — positional stratification across CMR `revision_date`, including offset computation, `_hits()` fallback when count is missing, and the ±1 retry on empty bins.
- `test_prefetch.py` — happy path; cap-overshoot at collection boundary; `--max-granule-size` skips oversized collections wholesale; `--collection` filter; `size_bytes` backfill on first download.
- `test_cubability.py` — the seven-step feasibility check, with fixtures for each failing-step case.
- `test_overrides.py` — round-trips a representative TOML; rejects typo concept IDs, hallucinated kwarg names, contradictory `skip_dataset` + `dataset = {...}`, unknown top-level keys, missing `notes`. `for_collection` returns empty defaults for unknown IDs.
- `test_inspect.py` — tiny generated fixtures in `tests/fixtures/inspect/` (HDF5, Zarr v3, NetCDF3, GeoTIFF, DMR++); asserts the dumped tree contains expected keys and snapshots the fenced JSON blob portion.
- `test_probe.py` — `resolve_target` honors the local state and falls back to CMR with the documented call counts; native-mode `generate_script` emits section markers in order, omits the collection block for granule input, comments out the inspect call when format isn't sniffed, and produces output that passes `python -m py_compile`.
- `test_main.py` — CLI integration: `discover --list` modes, `investigate --mode native|virtual` compiles, etc.
- `test_script_template.py` — each renderer produces compilable snippets; `render_login_and_store` dispatches on URL scheme and rejects unknown schemes.

### Integration (`tests/integration/`, opt-in)

One smoke test that runs the full pipeline on 3 collections with real EDL credentials. Skipped cleanly when `~/.netrc` is absent. Not run in CI.

## Extensibility

### Refining the taxonomy

After a survey run, read the "Top 50 Raw Errors in `OTHER`" section of the rendered report. For each recurring pattern:

- Add a `Bucket` value in `core/taxonomy.py` if it's a novel failure mode.
- Add a `(type_regex, message_regex, bucket)` rule at the correct position in `_RULES` (first match wins).
- Add a test case in `tests/unit/test_taxonomy.py`.
- Re-run `render`; no need to re-run `attempt`.

### Adding a format family

- Add a `FormatFamily` value in `core/formats.py`.
- Add declared-string and extension entries in `_DECLARED` and `_EXT`.
- Add a parser-dispatch branch in `pipeline._attempt.dispatch_parser` if VirtualiZarr supports it; otherwise attempts record `NoParserAvailable` automatically.

### Adding an access mode

- Add a branch in `pipeline._stores.StoreCache.get_store` and whatever store construction is needed (`make_https_store`, an S3-flavour helper, etc.).
- Extend the `--access` `click.Choice` in the relevant `cli/commands/*.py` modules.
- `cmr._sample._extract_urls` records both `s3_url` and `https_url` per granule today; if a third URL flavour is needed it joins as a new column on `GranuleRow` (bump `SCHEMA_VERSION` and re-run `discover && sample`).
- `pipeline._attempt._build_registry` keys on `scheme://netloc`; a new URL scheme should work without changes.

## Known limitations

- **Sequential only.** Single-process, single-threaded apart from the per-attempt timeout worker. At roughly 5 s/attempt and ~10k attempts, a full survey takes a workday. Natural partition for future parallelism is the DAAC.
- **No schema migrations.** State schema changes require deleting `output/state.json` and `output/results/` and re-running.
- **Taxonomy drift.** Upstream error strings change; the regex-based classifier needs maintenance whenever VirtualiZarr or its dependencies evolve. The `OTHER` bucket plus its raw-error drill-down is the operational mitigation.
- **External-mode redirects.** `HTTPStore`'s bearer header is attached as a default header per hostname; whether obstore preserves that header across 302 redirects to a different host (e.g. presigned S3) has not been exhaustively tested against the full DAAC matrix. Edge-case failures surface as bucketed errors in the report.
- **Timeout leaks threads.** A timed-out worker continues to run in the background until the interpreter exits. The daemon flag ensures eventual cleanup but the leaking thread can still hold sockets, memory, or file descriptors for the rest of the run.
- **Hierarchical datasets: partial support.** Phase 4b (Datatreeability) captures whether `ManifestStore.to_virtual_datatree()` succeeds, so collections that fail Phase 4a with `CONFLICTING_DIM_SIZES` or similar can still be counted as hierarchical-readable. The Cubability check (Phase 5) still operates on `xr.Dataset` fingerprints only; extending it to per-node `xr.DataTree` fingerprints (picking a representative node, or checking every node) is a future work item.
- **Fingerprint lossiness.** The per-granule fingerprint records codec *type names* and assumes uniform chunking along each dim. Collections whose per-variable codec parameters or chunking vary in ways not captured by the fingerprint will pass the cubability check but may still fail a real concat. Flagged explicitly as a trust boundary for reviewers.
- **VirtualiZarr parser coverage.** HDF4 lands in `NoParserAvailable` by design. Future VirtualiZarr releases that close that gap will be picked up automatically on the next run.

## Open questions for reviewers

CMR experts:

1. Is `FileDistributionInformation.Format → FileArchiveInformation.Format → probe-one-granule` the right precedence, or are there collections where only a product-type field reliably identifies the format?
2. Are there DAACs where `DataGranule.data_links(access="direct")` returns a non-S3 scheme (e.g. TEA-signed HTTPS treated as "direct"), and should we classify those differently?
3. Is the EOSDIS provider snapshot in `cmr/_providers.py` missing any active cloud-hosted DAAC? (Last audited Q1 2026.)
4. Resolved: switched to `revision_date`-based positional stratification. Reprocessing campaigns that change codecs appear at production time, not observation time; sorting by `revision_date` places old-codec and new-codec granules in different bins by construction. CMR guarantees `revision_date` is set on every granule, so no collection requires a fallback path.

VirtualiZarr experts:

1. Is splitting `parser(...)` from `to_virtual_dataset()` a fair and stable API contract, or do some parsers blur the line (e.g. by deferring work until dataset construction)?
2. Is the registry shape (`{f"{scheme}://{netloc}": store}`) sufficient for every current parser? DMR++ in particular can reference sidecar URLs — do those need a broader registry?
3. Is the fingerprint faithful enough for cubability? What's the minimum additional metadata (codec parameters, fill-value comparison, time-unit handling) that would let us trust a `FEASIBLE` verdict as a real concat?
4. For `VirtualTIFF`: does it integrate with `ObjectStoreRegistry` cleanly, and should we expect different error shapes from it than from the in-tree parsers?
5. Should failures like `not a valid HDF5 file` after a successful `HDFParser` dispatch be attributed to parser misdispatch (i.e. the declared format was wrong) or to genuine file corruption? The current taxonomy lumps them together under `CANT_OPEN_FILE`.

## Related prior art

- `titiler-cmr-compatibility` ran a structurally similar survey for tile-generation compatibility. Ported: EOSDIS provider filter, initial taxonomy buckets, UMM-JSON fields to record (processing level, short name + version), declared-format / extension mapping, Parquet-incremental-write pattern, per-granule timeout.
- Diverged: 5 stratified granules vs their 1 random granule per collection (for intra-collection heterogeneity detection); `open_virtual_dataset` split into parse + dataset phases vs their single `CMRBackend` tile-render test (different failure surface); cubability as a third phase vs their single-granule result.
