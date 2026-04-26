# Survey design

## Purpose

Measure, for each cloud-hosted NASA CMR collection of an array-like format, how far the stack gets when asked to virtualize it. The pipeline runs in five phases, of which 3, 4a, 4b, and 5 are the substantive measurement points (1 and 2 are setup):

1. **Discover** (Phase 1): enumerate CMR collections into DuckDB.
2. **Sample** (Phase 2): pick N granules per collection, stratified across the temporal extent.
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

Single CLI `nasa-virtual-zarr-survey` with five phase subcommands plus a `pilot` convenience wrapper. Phases share state through a DuckDB checkpoint DB and a DAAC-partitioned Parquet dataset.

```
earthaccess.search_datasets(cloud_hosted=True, provider=<EOSDIS>)
  ↓
discover → collections (DuckDB)                       [Phase 1]
  ↓
sample   → granules  (DuckDB)                         [Phase 2]
  ↓
attempt  → results.parquet, one row per granule       [Phases 3, 4a, 4b]
  ├─ Parsability:      parser(url=url, registry=...)
  ├─ Datasetability:   manifest_store.to_virtual_dataset()      (4a)
  └─ Datatreeability:  manifest_store.to_virtual_datatree()     (4b, parallel with 4a)
  ↓
report   → report.md + figures                        [Phase 5: cubability rollup]
```

- `discover` and `sample` are idempotent and safe to re-run.
- `attempt` is resumable: it skips `(collection, granule)` pairs already present in the Parquet log.
- `report` is cheap and side-effect-free; re-run after any taxonomy refinement.

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

For each eligible collection, `check_cubability(fingerprints)` runs a sequence of pass/fail checks:

1. Variable name sets match across granules.
2. Per-variable dtype / dims / codecs match.
3. A concat dimension can be unambiguously identified (preferring a size-varying dim; falling back to a dim with differing coord value hashes).
4. All non-concat dim sizes match.
5. All non-concat coord value hashes match.
6. Per-variable chunk sizes on non-concat axes match.
7. Concat-dim coord ranges are monotonic and non-overlapping across granules.

Verdicts: `FEASIBLE`, `INCOMPATIBLE`, `INCONCLUSIVE` (e.g. ambiguous concat dim, all granules identical), `NOT_ATTEMPTED` (Phase 4 didn't fully pass).

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

### Sampling

For each array-like collection, 5 granules are sampled.

- **Stratified (preferred):** split the temporal extent into 5 equal bins; for each bin, call `earthaccess.search_data(concept_id, temporal=(bin_start, bin_end), count=1)`. If a bin returns no granules, it is silently dropped (fewer than 5 rows but still `stratified=True`).
- **Fallback:** if the temporal extent is missing, call `search_data(concept_id, count=5)` and assign synthetic `temporal_bin=0..4` with `stratified=False`.

The `stratified` flag propagates to the Parquet log so the final report can distinguish genuine temporal coverage from fallback sampling.

Each granule row also records the `access_mode` it was sampled under (`direct` or `external`). On a re-run with a different `--access`, `run_sample` deletes existing rows whose mode does not match and re-samples those collections so the URL scheme in the granules table always agrees with the requested mode. A warning is logged listing the affected collections so the operator knows existing rows in `output/results/*.parquet` still reference the old URLs.

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

Re-running `attempt` after a VirtualiZarr bump, a taxonomy refinement, or a new override means re-fetching bytes for every sampled granule. The same applies to `repro` iteration on a single granule's parser kwargs and to `pilot --sample N` while developing taxonomy rules. Most failures the survey records are properties of the granule (parser bug, malformed file, unsupported codec) rather than the network path, so caching fetched bytes between iterations does not mask the signal.

A new module `cache.py` provides `DiskCachingReadableStore`, an `obspec` wrapper that persists fetched bytes to `<cache_dir>/<scheme>/<host>/<sha256(scheme://host/path)>`. `auth.StoreCache.get_store` wraps every constructed store in it when caching is enabled, so `_build_registry` and the rest of the pipeline are unchanged. Three CLI flags on `attempt`, `pilot`, and generated repro scripts:

- `--cache / --no-cache` — default off. The cache silently consumes disk; opt-in.
- `--cache-dir PATH` — default `~/.cache/nasa-virtual-zarr-survey/`. Honors `NASA_VZ_SURVEY_CACHE_DIR`.
- `--cache-max-size SIZE` — default `50GB`, accepts human-readable strings (`50GB`, `2.5TB`).

### Cache key

`sha256(scheme://host/path)`. Stable for the survey's purposes: granule URLs do not change for a given concept ID across runs. `direct` and `external` access modes produce different URLs for the same logical granule and so end up with different cache entries — correct, since DAAC HTTPS gateways often redirect to presigned S3 and the bytes returned can differ.

### Data flow

A read through a wrapped store:

1. Caller asks `wrapped.get(path)` or `wrapped.get_range(path, start, end)`.
2. Compute `local_path = cache_dir / scheme / host / sha256(scheme://host/path)`.
3. **Cache hit** (file exists): serve from the local file, no network.
4. **Cache miss**:
   - `head(path)` against the underlying store to learn the object size.
   - If `current_size + object_size <= max_bytes`: fetch the full object via `store.get(path)`, write atomically (`*.tmp` + `os.replace`), update the in-memory size counter under a `threading.Lock`, then serve the read from disk.
   - If the cap would be exceeded: log a warning once per process, fall through to the underlying store without caching. Survey continues.

The `head`-before-fetch costs one extra round trip per cache miss but is the only way to honor the cap without speculatively writing then deleting. On `DiskCachingReadableStore` init, the cache dir is walked once to compute current size from disk — authoritative, slightly slow on a 50 GB cache, runs once per process.

### Cap warning

Printed once per process to stderr when the cap is first exceeded:

```
[cache] cache size 49.8 GB exceeds cap 50.0 GB;
        further granules will not be cached.
        clear the cache with `rm -rf <cache_dir>`
        or pass --cache-max-size to raise the cap.
```

`<cache_dir>` is interpolated from the actual `--cache-dir` so the message is copy-pasteable.

### Failure modes

- **Cache dir not writable** (read-only FS, permission error on first write): log once, fall through to direct fetches. Survey still completes, uncached.
- **Disk full mid-write**: catch `OSError`, delete the partial `*.tmp`, log warning, fall through.
- **Underlying store error during a cache-miss fetch**: propagated unchanged. The wrapper is transparent — no negative caching.
- **Concurrent writers** across worktrees: atomic `*.tmp` + `os.replace` ensures readers always see a complete file or none. The in-process `Lock` covers size accounting only.
- **Stale `.tmp`** from a crashed prior run: ignored on read, overwritten on next write.

### Caveats

- *First-fetch timeout interaction.* With `--cache` on, the first read of a granule downloads the full file even when the parser only needs a few MB. That fetch happens inside the parse phase under `--timeout` (default 60 s), so granules whose full size is hundreds of MB on a slow link can newly time out. Mitigation: raise `--timeout` for the first cached run; subsequent runs serve from disk.
- *First-run wall time.* The first cached run downloads more bytes than an uncached run; the break-even is iteration two.
- *Sequential safety only.* Atomic rename + in-process lock is sufficient for single-process runs and occasional worktree overlap, not a future ProcessPool.

### Non-goals

- Range-level caching. v1 is whole-granule; if a parser only reads 5 MB of a 1 GB HDF5 file, v1 still downloads the full GB on first miss. Range-level caching can come later if profiling shows giant files dominate.
- Negative caching. Failed responses (403, 404, timeouts) are never written.
- Auto-eviction. Append-only; the operator clears the cache when the cap warning fires.
- Cross-process locking. Atomic writes are sufficient for the workloads we care about.
- Caching at non-granule layers (CMR JSON responses, EDL credentials). Cheap relative to granule fetches and out of scope.
- Built-in `cache info` / `cache clear` subcommands. `du -sh` and `rm -rf` against `<cache_dir>` suffice.

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

Loaded eagerly at the top of `run_attempt`, so a malformed entry fails the run before any granule is touched.

1. Top-level keys match `^C\d+-[A-Z]+$`.
2. Per-collection sub-keys limited to: `parser`, `dataset`, `datatree`, `skip_dataset`, `skip_datatree`, `notes`. Anything else rejected.
3. `parser` / `dataset` / `datatree` values must be inline tables (or absent).
4. `parser` kwarg names checked against the dispatched parser's `__init__` signature for that collection's format family. A `group=` on a `NoParserAvailable` family is an error.
5. `dataset` kwargs validated against `ManifestStore.to_virtual_dataset`'s signature; `datatree` against `to_virtual_datatree`'s.
6. `skip_dataset = true` combined with `dataset = {...}` is an error (contradictory). Same for datatree.
7. `notes` is required on any non-empty entry — a one-line rationale for review.

`nasa-virtual-zarr-survey validate-overrides` runs the rules standalone and prints `OK` or the first error with a useful pointer.

### Module layout

`overrides.py` is the only new code surface for the mechanism:

- `OverrideRegistry.from_toml(path) -> OverrideRegistry` parses and validates.
- `registry.for_collection(concept_id) -> CollectionOverride` returns an immutable record carrying `parser_kwargs`, `dataset_kwargs`, `datatree_kwargs`, `skip_dataset`, `skip_datatree`. A miss returns the empty default.
- `apply_to_parser(parser_cls, kwargs)`, `apply_to_dataset_call(manifest_store, kwargs)`, `apply_to_datatree_call(manifest_store, kwargs)` are used by both `attempt_one` and the repro renderer so application logic lives in one place.

`attempt_one` accepts `override: CollectionOverride | None = None` (defaulting to no-op); `run_attempt` loads the registry once at startup and passes the matching override into each call. The worker thread and timeout logic are untouched.

### Effect on results

`AttemptResult` and the Parquet schema gain a single new column, `override_applied: bool`, true when any kwarg or skip flag was used. The report distinguishes "succeeded naively" from "succeeded with an override" — different signals to a downstream reader. Override kwargs themselves are *not* serialized into Parquet; the TOML file plus the git SHA at run time are the canonical record.

### Non-goals

- Per-granule overrides. The granularity is collection only.
- A CLI to edit the override file. TOML is short and structured; a CLI for "set a key in a TOML file" is friction.
- Backporting overrides to existing failed result rows. A re-run after editing the file is the supported workflow.

## Repro, probe, and structural inspection

The survey ships two complementary diagnostic surfaces, both of which emit a runnable Python script as their artifact.

### `repro` — reproduce a recorded failure

`repro.py` generates a self-contained script per failing granule that reproduces the failing operation against the same URL, parser, and kwargs the survey used. The renderer reads `collection_overrides.toml` and bakes any existing overrides into the script, so the repro mirrors current attempt behavior; `--no-overrides` forces the script to mirror an unconfigured run, useful when first investigating a regression.

The generated script also doubles as a working starting point for non-debugging virtualization workflows: edit the parser/dataset kwargs (or strip the failure-context docstring) and treat it as a runnable seed.

If `repro CONCEPT_ID` finds no matching failures because the collection was skipped at discover time (`skip_reason='format_unknown'`) or has no granules sampled / no Parquet rows, the error message points you at `probe`.

### `probe` — investigate any concept ID

`probe.py` is the diagnostic counterpart. It takes one concept ID (`C...` or `G...`, auto-detected by prefix) and emits a script that logs in, dumps the collection / granule UMM-JSON, prints both `direct` and `external` data links, constructs an obstore-backed store, and (when format can be sniffed from the URL extension) calls `inspect_url` for a structural dump.

`probe` prefers the local survey DB and falls back to one or two CMR calls only when the concept ID is absent:

| Input | Local DB state | CMR calls at gen time |
|---|---|---|
| `G456` | granule in DB | 0 |
| `G456` | granule not in DB | 1 (`search_data`) |
| `C123` | collection + granules in DB | 0 |
| `C123` | collection in DB, no granules | 1 (`search_data`) |
| `C123` | collection not in DB | 2 (`search_datasets` + `search_data`) |

Probe is per-target by design — the script always inspects exactly one granule. To look at more granules, re-run `probe` with another granule ID. To force a CMR-free path, hand-edit the generated script.

### `inspect.py` — per-format structural dispatch

The shared `inspect.py` module dispatches per `FormatFamily`:

- **HDF5 / NetCDF4** — `h5py` walk: per group, list datasets with `shape`, `dtype`, `chunks`, `compression`, `compression_opts`, attached dim scales, fillvalue, top 10 attrs.
- **Zarr** — open via `zarr-python`, dump `zarr.json` hierarchy: arrays with shape/dtype/chunks/codecs and group attrs.
- **NetCDF3** — `netCDF4` (or `scipy.io.netcdf`) for dims, vars, and attrs.
- **GeoTIFF / COG** — `tifffile`: IFDs, predictor, compression, tile/strip layout, photometric, GeoKeys.
- **DMR++** — parse the XML and dump shape/dtype/chunk/filter info.
- **FITS** — `astropy.io.fits.info()` plus per-HDU header extract.

Two principles: the inspector reads through the same `obstore` `S3Store` / `HTTPStore` the parser uses (no separate auth path), and output is human-readable text *and* machine-parseable — a JSON blob block at the end of stdout, fenced with `<<<INSPECT_JSON_BEGIN>>>` ... `<<<INSPECT_JSON_END>>>` sentinels for downstream tooling.

### Shared script template

Both `repro` and `probe` compose their output from `script_template.py`, which exposes pure string-snippet emitters: `render_cache_argparse`, `render_earthaccess_login`, `render_store` / `render_login_and_store`, `render_cache_wiring`, `render_inspect_block`. This keeps the login / store / cache blocks identical across the two CLIs.

### Migration: `--inspect` / `--attempt` removed

Earlier versions of `repro` emitted scripts with a `--inspect` / `--attempt` argparse mutex on the generated script: `--inspect` ran the structure dump only; `--attempt` ran the virtualization only. Both flags are gone. `probe` now owns structural inspection, so a `repro G456` run that previously needed `--inspect` becomes `probe G456` instead. Existing checked-in `repro_*.py` scripts continue to work — they're standalone, not regenerated.

### The debug loop

1. Naive `attempt` records `C123-DAAC` failing with e.g. `CONFLICTING_DIM_SIZES`, `override_applied = false`.
2. `nasa-virtual-zarr-survey repro C123-DAAC` emits `repro_G456.py` with current overrides baked in (or `--no-overrides`).
3. Run the script. Structure dump prints, then the failing operation. Edit kwargs in the script until something works, or conclude it is an upstream bug.
4. Hand-edit `config/collection_overrides.toml`; add a section for `C123-DAAC` with `notes` linking to the repro filename or upstream issue.
5. `validate-overrides` to confirm.
6. Re-run scoped: `attempt --collection C123-DAAC` (the per-collection equivalent of `--daac`). Existing `_pending_granules` logic skips already-attempted granules, so a re-attempt requires removing that collection's rows from the Parquet log first — the manual operation is `find output/results/ -name '*.parquet' -delete` for a small survey.
7. Full re-run records `override_applied = true`.

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

### DuckDB (`output/survey.duckdb`)

```sql
CREATE TABLE collections (
  concept_id       TEXT PRIMARY KEY,
  short_name       TEXT,
  version          TEXT,
  daac             TEXT,
  provider         TEXT,
  format_family    TEXT,      -- one of FormatFamily, NULL if unknown / non-array
  format_declared  TEXT,      -- raw CMR-declared string, for debugging
  num_granules     BIGINT,
  time_start       TIMESTAMP,
  time_end         TIMESTAMP,
  processing_level TEXT,
  skip_reason      TEXT,      -- NULL | 'non_array_format' | 'format_unknown'
  discovered_at    TIMESTAMP
);

CREATE TABLE granules (
  collection_concept_id TEXT NOT NULL,
  granule_concept_id    TEXT NOT NULL,
  data_url              TEXT,
  temporal_bin          INTEGER,
  size_bytes            BIGINT,
  sampled_at            TIMESTAMP,
  stratified            BOOLEAN,
  access_mode           TEXT NOT NULL,  -- 'direct' | 'external'
  PRIMARY KEY (collection_concept_id, granule_concept_id)
);
```

Schema creation uses `CREATE TABLE IF NOT EXISTS`. No migrations: schema changes require deleting `output/survey.duckdb` and `output/results/` and re-running.

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
| `stratified` | BOOL | propagated from sampling |
| `fingerprint` | STRING | JSON; populated only on full success |
| `attempted_at` | TIMESTAMP | UTC |

Shards rotate every 500 rows (`ResultWriter.shard_size`). A SIGINT handler flushes every DAAC's buffered rows before exiting with code 0, so reruns resume cleanly.

### Resume logic

`_pending_granules` joins `granules` with `collections` and excludes pairs already present in the Parquet log:

```sql
WHERE c.skip_reason IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM read_parquet(?, union_by_name=true, hive_partitioning=true) r
    WHERE r.collection_concept_id = g.collection_concept_id
      AND r.granule_concept_id   = g.granule_concept_id
  )
```

On the first run, `read_parquet` raises (no files yet); a `try/except` falls back to "all pending granules." Results are ordered by `(daac, collection, temporal_bin)` so per-collection progress lines to stderr are meaningful and shards stay DAAC-local.

## Reporting

`report.py` reads `survey.duckdb` and the full Parquet log via DuckDB's `read_parquet` with Hive partitioning. The emitted Markdown contains:

- Overview Sankey (collections → parsable → datasetable → cubable).
- Per-phase verdict tables (`all_pass` / `partial_pass` / `all_fail` / `not_attempted` / `skipped`). `skipped` is assigned at the collection level from `collections.skip_reason`; the other verdicts are derived from the Parquet log.
- Per-phase failure taxonomy (see [Failure Taxonomy](taxonomy.md)) with both granule and distinct-collection counts.
- Per-DAAC and per-format-family tables in the form "parsable / datasetable / cubable (% of the previous column)."
- Stratification breakdown (stratified vs fallback vs unsampled).
- Top-50 raw errors per phase for the `OTHER` bucket, seeding the next round of taxonomy refinement.
- Full per-collection table at the end.

## Error handling

- **Per-attempt:** all exceptions caught; errors serialized into the Parquet row. Timeouts → `TimeoutError`. Auth failures → `AuthUnavailable`.
- **No retries.** Flakiness surfaces naturally as `partial_pass` across the 5 stratified granules. Retrying inside `attempt_one` would conflate transient failure with genuine lack of support and muddy the taxonomy.
- **Process-level SIGINT:** flushes every DAAC's active shard, then `sys.exit(0)`. The next `attempt` run resumes from where it stopped.
- **Forbidden run-abort:** 5 consecutive direct-mode 403s cause a clean abort with a remediation message. Non-direct runs do not trigger this.

## CLI

Click-based. Subcommands: `version`, `discover`, `sample`, `attempt`, `report`, `pilot`, `repro` (minimizes any single failure into a runnable script), `probe` (emits a runnable script for investigating any concept ID, even one with no recorded failures), and `validate-overrides` (loads `config/collection_overrides.toml` and prints `OK` or the first validation error).

Common flags across work phases:

- `--db PATH` — DuckDB checkpoint (default `output/survey.duckdb`)
- `--results PATH` — Parquet results directory (default `output/results`)
- `--out PATH` — report output (default `docs/results/index.md`)
- `--access {direct,external}` — granule access mode (default `direct`)
- `--daac NAME` — restrict to one DAAC
- `--collection CONCEPT_ID` — restrict to one collection (per-collection equivalent of `--daac`)

Discovery-specific:

- `--limit N`, `--top N`, `--top-per-provider N` (mutually exclusive)
- `--dry-run`, `--skipped`

Attempt-specific:

- `--timeout SECONDS` (default 60)
- `--shard-size ROWS` (default 500)

Cache-related, on `attempt`, `pilot`, and generated repro scripts:

- `--cache / --no-cache` — default off
- `--cache-dir PATH` — default `~/.cache/nasa-virtual-zarr-survey/` (also `NASA_VZ_SURVEY_CACHE_DIR`)
- `--cache-max-size SIZE` — default `50GB`, accepts human-readable strings

Repro-specific (on the `repro` subcommand):

- `--no-overrides` — generate / run as if `collection_overrides.toml` were empty
- `--bucket NAME`, `--phase {parse,dataset}`, `--limit N`

Probe-specific (on the `probe` subcommand):

- `--access {direct,external}` — selects the data link the script binds to
- `--out PATH` — write `probe_<id>.py` to a directory; default stdout

The `pilot` subcommand runs all phases end-to-end on a small sample (`--sample N`, default 50) so users can review raw errors and refine `taxonomy.py` before committing to a full survey.

`__main__.py` suppresses three noisy upstream warnings (`earthaccess` `DataGranule.size` `FutureWarning`, Numcodecs / Imagecodecs "not in Zarr v3 spec" `UserWarning`s) to keep stderr meaningful.

## Testing

### Unit (`tests/unit/`)

Every module has mocked tests for its public API. Notable suites:

- `test_taxonomy.py` — table-driven, one case per hypothesized bucket; grows as the pilot reveals new patterns.
- `test_auth.py` — both modes mocked at `earthaccess`, `obstore.store.S3Store`, and `HTTPStore`. Does not exercise real S3 or real EDL. Also covers `StoreCache(cache_dir=...)` wrapping returned stores in `DiskCachingReadableStore`.
- `test_attempt.py::test_run_attempt_resumes` — pre-populates a Parquet shard, verifies the resume check skips already-attempted granules.
- `test_attempt.py` (cache + override paths) — second-run with `--cache --cache-dir` issues zero network calls to the underlying store; `override_applied` is recorded correctly when an override is matched.
- `test_sample.py` — both the stratified-bins branch and the no-temporal-extent fallback.
- `test_cubability.py` — the seven-step feasibility check, with fixtures for each failing-step case.
- `test_overrides.py` — round-trips a representative TOML; rejects typo concept IDs, hallucinated kwarg names, contradictory `skip_dataset` + `dataset = {...}`, unknown top-level keys, missing `notes`. `for_collection` returns empty defaults for unknown IDs.
- `test_inspect.py` — tiny generated fixtures in `tests/fixtures/inspect/` (HDF5, Zarr v3, NetCDF3, GeoTIFF, DMR++); asserts the dumped tree contains expected keys and snapshots the fenced JSON blob portion.
- `test_cache.py` — round-trip `get` then `get` again serves from disk; `get_range` after a full `get` serves from cache; cap exceeded → no file written, fall through, warning logged once; two `DiskCachingReadableStore` instances against the same dir share state; atomic write under simulated crash leaves only `*.tmp`; underlying store error during a cache-miss fetch leaves no `*.tmp` and propagates.
- `test_repro.py` — generated scripts pass `python -m py_compile`, contain the cache `argparse` block, and bake the right kwargs in when overrides exist for the collection. The docstring points at `probe` for structural inspection.
- `test_probe.py` — `resolve_target` honors the local DB and falls back to CMR with the documented call counts; `generate_script` emits section markers in order, omits the collection block for granule input, comments out the inspect call when format isn't sniffed, and produces output that passes `python -m py_compile`.
- `test_script_template.py` — each renderer produces compilable snippets; `render_login_and_store` dispatches on URL scheme and rejects unknown schemes.

### Integration (`tests/integration/`, opt-in)

One smoke test that runs the full pipeline on 3 collections with real EDL credentials. Skipped cleanly when `~/.netrc` is absent. Not run in CI.

## Extensibility

### Refining the taxonomy

After a pilot run, read the "Top 50 Raw Errors in `OTHER`" section of `output/report.md`. For each recurring pattern:

- Add a `Bucket` value in `taxonomy.py` if it's a novel failure mode.
- Add a `(type_regex, message_regex, bucket)` rule at the correct position in `_RULES` (first match wins).
- Add a test case in `tests/unit/test_taxonomy.py`.
- Re-run `report`; no need to re-run `attempt`.

### Adding a format family

- Add a `FormatFamily` value.
- Add declared-string and extension entries in `formats._DECLARED` and `_EXT`.
- Add a parser-dispatch branch in `attempt.dispatch_parser` if VirtualiZarr supports it; otherwise attempts record `NoParserAvailable`.

### Adding an access mode

- Add a branch in `StoreCache.get_store` and whatever store construction is needed.
- Extend `--access` `click.Choice`.
- `sample._extract_url` already forwards the mode to `data_links(access=...)`.
- `_build_registry` keys on `scheme://netloc`; a new scheme should work without changes.

## Known limitations

- **Sequential only.** Single-process, single-threaded apart from the per-attempt timeout worker. At roughly 5 s/attempt and ~10k attempts, a full survey takes a workday. Natural partition for future parallelism is the DAAC.
- **No schema migrations.** DB schema changes require deleting `output/survey.duckdb` and `output/results/` and re-running.
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
3. Is the EOSDIS provider snapshot in `providers.py` missing any active cloud-hosted DAAC? (Last audited Q1 2026.)
4. Are there collections where temporal extent splitting is misleading — e.g. reprocessing campaigns that invalidate earlier bins? Should we prefer `reprocessed=true` filtering?

VirtualiZarr experts:

1. Is splitting `parser(...)` from `to_virtual_dataset()` a fair and stable API contract, or do some parsers blur the line (e.g. by deferring work until dataset construction)?
2. Is the registry shape (`{f"{scheme}://{netloc}": store}`) sufficient for every current parser? DMR++ in particular can reference sidecar URLs — do those need a broader registry?
3. Is the fingerprint faithful enough for cubability? What's the minimum additional metadata (codec parameters, fill-value comparison, time-unit handling) that would let us trust a `FEASIBLE` verdict as a real concat?
4. For `VirtualTIFF`: does it integrate with `ObjectStoreRegistry` cleanly, and should we expect different error shapes from it than from the in-tree parsers?
5. Should failures like `not a valid HDF5 file` after a successful `HDFParser` dispatch be attributed to parser misdispatch (i.e. the declared format was wrong) or to genuine file corruption? The current taxonomy lumps them together under `CANT_OPEN_FILE`.

## Related prior art

- `titiler-cmr-compatibility` ran a structurally similar survey for tile-generation compatibility. Ported: EOSDIS provider filter, initial taxonomy buckets, UMM-JSON fields to record (processing level, short name + version), declared-format / extension mapping, Parquet-incremental-write pattern, per-granule timeout.
- Diverged: 5 stratified granules vs their 1 random granule per collection (for intra-collection heterogeneity detection); `open_virtual_dataset` split into parse + dataset phases vs their single `CMRBackend` tile-render test (different failure surface); cubability as a third phase vs their single-granule result.
