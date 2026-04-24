# Survey design

## Purpose

Measure, for each cloud-hosted NASA CMR collection of an array-like format, how far the stack gets when asked to virtualize it — broken into three independently-observable phases:

1. **Parsability** — can a VirtualiZarr parser read the file's metadata into a `ManifestStore`?
2. **Datasetability** — can that `ManifestStore` be materialized into an `xarray.Dataset` via `ManifestStore.to_virtual_dataset()`?
3. **Cubability** — across N stratified granules of a collection, are the per-granule datasets structurally compatible enough to be concatenated into one coherent virtual cube?

The output is a per-collection verdict table plus a Markdown report that VirtualiZarr maintainers and NASA DAAC operators can act on.

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

Click-based. Subcommands: `version`, `discover`, `sample`, `attempt`, `report`, `pilot`, plus a `repro` helper that minimizes any single failure into a runnable script.

Common flags across work phases:

- `--db PATH` — DuckDB checkpoint (default `output/survey.duckdb`)
- `--results PATH` — Parquet results directory (default `output/results`)
- `--out PATH` — report output (default `docs/results/index.md`)
- `--access {direct,external}` — granule access mode (default `direct`)
- `--daac NAME` — restrict to one DAAC

Discovery-specific:

- `--limit N`, `--top N`, `--top-per-provider N` (mutually exclusive)
- `--dry-run`, `--skipped`

Attempt-specific:

- `--timeout SECONDS` (default 60)
- `--shard-size ROWS` (default 500)

The `pilot` subcommand runs all phases end-to-end on a small sample (`--sample N`, default 50) so users can review raw errors and refine `taxonomy.py` before committing to a full survey.

`__main__.py` suppresses three noisy upstream warnings (`earthaccess` `DataGranule.size` `FutureWarning`, Numcodecs / Imagecodecs "not in Zarr v3 spec" `UserWarning`s) to keep stderr meaningful.

## Testing

### Unit (`tests/unit/`)

Every module has mocked tests for its public API. Notable suites:

- `test_taxonomy.py` — table-driven, one case per hypothesized bucket; grows as the pilot reveals new patterns.
- `test_auth.py` — both modes mocked at `earthaccess`, `obstore.store.S3Store`, and `HTTPStore`. Does not exercise real S3 or real EDL.
- `test_attempt.py::test_run_attempt_resumes` — pre-populates a Parquet shard, verifies the resume check skips already-attempted granules.
- `test_sample.py` — both the stratified-bins branch and the no-temporal-extent fallback.
- `test_cubability.py` — the seven-step feasibility check, with fixtures for each failing-step case.

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
