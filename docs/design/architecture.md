# nasa-virtual-zarr-survey: Design

*As-built design of the nasa-virtual-zarr-survey tool, April 2026.*

## Purpose

Measure how many cloud-hosted NASA CMR collections can be opened with `virtualizarr.open_virtual_dataset`, and categorize the failures into actionable buckets (missing parser, variable-length chunks, unsupported codec, auth, etc.). Output a per-collection verdict table and a Markdown report that VirtualiZarr maintainers and NASA DAAC operators can act on.

## Scope

- **In:** NASA CMR collections with `cloud_hosted=True` hosted by EOSDIS DAACs, whose declared format is array-like (NetCDF3/4, HDF4/5, Zarr, GeoTIFF, FITS, DMR++).
- **Out:** on-prem-only collections, non-array formats (PDF, shapefile, CSV), non-NASA providers.

## Architecture

Single CLI `nasa-virtual-zarr-survey` with five phase subcommands plus a `pilot` convenience wrapper. Phases share state through a DuckDB checkpoint DB and a DAAC-partitioned Parquet dataset.

```
earthaccess.search_datasets(cloud_hosted=True, provider=<EOSDIS>)
  ↓
discover → collections (DuckDB)
  ↓
sample   → granules (DuckDB)      (stratified temporal sampling)
  ↓
attempt  → results.parquet         (per-granule open_virtual_dataset attempts)
  ↓
report   → report.md
```

- `discover` and `sample` are idempotent and safe to re-run.
- `attempt` is resumable: it skips (collection, granule) pairs already present in the Parquet log.
- `report` is cheap and side-effect-free; re-run after any taxonomy refinement.

## Components

All modules live in `src/nasa_virtual_zarr_survey/`.

### Phases

#### Phase 1: `discover.py`

Calls `earthaccess.search_datasets(cloud_hosted=True, provider=<EOSDIS list>, count=<limit>)` and writes one row per collection to DuckDB. Extracts format, DAAC, provider, temporal extent, processing level, and granule count from UMM-JSON. Collections with non-array-like declared formats are persisted with `skip_reason = "non_array_format"` and skipped by later phases.

#### Phase 2: `sample.py`

For each pending collection:

- If the collection has a temporal extent (`time_start` and `time_end` populated), split it into N equal-width bins (default N=5) and issue one `earthaccess.search_data(concept_id, temporal=(start, end), count=1)` per bin. Each returned granule is stored with `stratified=True` and `temporal_bin=0..N-1`.
- If the temporal extent is missing, fall back to a single `search_data(concept_id, count=N)` call and store the returned granules with `stratified=False` and synthetic `temporal_bin=0..N-1`. This loses stratification guarantees but still provides some heterogeneity coverage for the attempt phase.

The `stratified` flag propagates through the rest of the pipeline so the final report can distinguish genuine temporal coverage from fallback sampling.

#### Phases 3 and 4: `attempt.py`

Two halves: the per-granule dispatcher (`attempt_one`) and the resume loop (`run_attempt`).

**Per-granule (`attempt_one`):**

- `dispatch_parser(family)` returns a VirtualiZarr parser instance for the format family, or `None` for families VirtualiZarr does not support (HDF4, GeoTIFF).
- When a parser exists, `attempt_one` calls `virtualizarr.open_virtual_dataset(url, registry, parser)` inside a `ThreadPoolExecutor.submit(...).result(timeout=60)`. On timeout, the future is abandoned (threads leak; acceptable because the run is sequential and parsers restart cleanly between DAACs).
- All exceptions are caught, serialized (`type(e).__name__`, `str(e)`, truncated traceback), and packaged into an `AttemptResult`. A failed attempt is a valid data point, never raises.

**Resume loop (`run_attempt`):**

- Queries `granules JOIN collections` for rows not yet present in `results.parquet` (via `NOT EXISTS (SELECT 1 FROM read_parquet(...))`).
- For each pending granule, mints a store via `StoreCache.get_store(provider, url)` and hands off to `attempt_one`.
- Writes results to a DAAC-partitioned Parquet shard (`results/DAAC=<daac>/part-NNNN.parquet`), rotating every 500 rows. A SIGINT handler flushes the active shard and exits with code 0 so reruns resume cleanly.
- Emits a heartbeat line to stderr every 500 attempts.

#### Phase 5: `report.py`

Reads `survey.duckdb` and `results.parquet` via DuckDB (`read_parquet` with Hive partitioning). Emits `report.md` with:

- Totals and per-verdict counts (`all_pass`, `partial_pass`, `all_fail`, `skipped_format`, `sample_failed`)
- Failure taxonomy counts (via `classify`)
- Per-DAAC breakdown
- Per-format-family breakdown
- Stratification breakdown (stratified / fallback / unsampled)
- Top 20 raw errors in the `OTHER` bucket (for ongoing taxonomy refinement)

Verdict rules:

- `skipped_format`: collection prefiltered during `discover` as non-array
- `sample_failed`: collection in DB but no granules attempted
- `all_pass`: every attempted granule succeeded
- `all_fail`: every attempted granule failed
- `partial_pass`: some attempts succeeded, some failed (heterogeneity signal)

### Helpers

#### `providers.py`

Snapshot of EOSDIS DAAC providers. Ported from `titiler-cmr-compatibility`. Pure function; re-check annually against `https://cmr.earthdata.nasa.gov/search/providers`.

#### `formats.py`

`FormatFamily` enum (`NETCDF4`, `NETCDF3`, `HDF5`, `HDF4`, `ZARR`, `GEOTIFF`, `FITS`, `DMRPP`) plus `classify_format(declared, url) -> FormatFamily | None`. Maps CMR-declared format strings and file extensions to array-like families. Collections that don't match any family are marked `skipped_format` during `discover`.

#### `db.py`

DuckDB schema for two checkpoint tables: `collections` and `granules`. Uses `CREATE TABLE IF NOT EXISTS` for idempotency. No migrations: the tool is designed to be re-run on a fresh DB when the schema changes.

#### `auth.py`

Two caches:

- `DAACStoreCache`: direct-S3 mode. `get_store(provider)` calls `earthaccess.get_s3_credentials(provider=...)` and wraps the result in an `obstore.store.S3Store`. Cached per CMR provider with a 50-minute TTL (credentials expire at 60 minutes).
- `StoreCache`: unified dispatcher that routes by access mode.
    - `access="direct"` delegates to `DAACStoreCache`.
    - `access="external"` logs into EDL, pulls the bearer token from `earthaccess.__auth__.token["access_token"]`, and hands out an `obspec_utils.stores.AiohttpStore` instance per hostname, with `Authorization: Bearer <token>` header.

Both raise `AuthUnavailable` on empty credentials or missing tokens. The caller records the failure as a per-granule `AuthUnavailable` result rather than aborting the run.

#### `taxonomy.py`

Empirically-derived classifier mapping `(error_type, error_message)` to a `Bucket` value. Seeded from `titiler-cmr-compatibility`'s `IncompatibilityReason` enum plus hypothesized VirtualiZarr-specific buckets, then refined as real errors surface in the `OTHER` bucket of a pilot run.

Rules are ordered `(error_type_pattern, error_message_pattern, bucket)` tuples with first-match-wins semantics. See the [Failure Taxonomy](taxonomy.md) reference for per-bucket descriptions, example error strings, and typical next steps.

#### `__main__.py`

Click CLI. Subcommands: `version`, `discover`, `sample`, `attempt`, `report`, `pilot`. Common flags across the work phases:

- `--db PATH`: DuckDB checkpoint file (default `output/survey.duckdb`)
- `--results PATH`: Parquet results directory (default `output/results`)
- `--out PATH`: report output path (default `output/report.md`)
- `--access {direct,external}`: CMR granule access mode; default `direct`
- `--daac NAME`: restrict to one DAAC

The `pilot` subcommand runs all phases end-to-end on a small sample (`--sample N`, default 50) so users can review raw errors and refine `taxonomy.py` before committing to a full survey.

Module-level `warnings.filterwarnings` calls at the top of `__main__.py` suppress two noisy upstream warnings (`earthaccess` `DataGranule.size` `FutureWarning`, `zarr` numcodecs `ZarrUserWarning`).

## Data Model

### DuckDB (`survey.duckdb`)

```sql
CREATE TABLE collections (
  concept_id       TEXT PRIMARY KEY,
  short_name       TEXT,
  version          TEXT,
  daac             TEXT,
  provider         TEXT,
  format_family    TEXT,
  format_declared  TEXT,
  num_granules     BIGINT,
  time_start       TIMESTAMP,
  time_end         TIMESTAMP,
  processing_level TEXT,
  skip_reason      TEXT,
  discovered_at    TIMESTAMP
);

CREATE TABLE granules (
  collection_concept_id TEXT,
  granule_concept_id    TEXT,
  data_url              TEXT,
  temporal_bin          INTEGER,
  size_bytes            BIGINT,
  sampled_at            TIMESTAMP,
  stratified            BOOLEAN,
  PRIMARY KEY (collection_concept_id, granule_concept_id)
);
```

### Parquet (`results/DAAC=<daac>/part-NNNN.parquet`)

Append-only per-attempt log. One row per `open_virtual_dataset` call.

| Column | Type | Notes |
|---|---|---|
| collection_concept_id | STRING | |
| granule_concept_id | STRING | |
| daac | STRING | partition key |
| format_family | STRING | |
| parser | STRING | `HDFParser` / `NetCDF3Parser` / `FITSParser` / `DMRPPParser` / `ZarrParser` |
| success | BOOL | |
| error_type | STRING | `type(e).__name__`, null on success |
| error_message | STRING | `str(e)`, truncated to 2 KB |
| error_traceback | STRING | truncated to 4 KB |
| duration_s | DOUBLE | wall time including parser init |
| timed_out | BOOL | true if killed by 60-second timeout |
| stratified | BOOL | propagated from the granule sampling mode |
| attempted_at | TIMESTAMP | UTC |

Partitioning by DAAC limits blast radius on crash: only the active shard's in-memory buffer is at risk, and per-DAAC shards make partial reprocessing easy.

## Access Modes

Two modes are supported end-to-end. The flag plumbs through `sample` (which URL to store) and `attempt` (which store type to use). The rest of the pipeline is mode-agnostic because `_build_registry` keys the registry by `scheme://netloc`, naturally handling both `s3://bucket` and `https://host`.

### `direct` (default)

- Granule URLs are S3 (`s3://bucket/key`), obtained via `DataGranule.data_links(access="direct")`.
- Requires AWS compute in `us-west-2` (NASA S3 direct access is region-locked).
- Store: `obstore.store.S3Store`, credentials minted per CMR provider by `earthaccess.get_s3_credentials`.

### `external`

- Granule URLs are HTTPS (`https://<daac-host>/path/file.nc`), obtained via `DataGranule.data_links(access="external")`.
- Works from anywhere with internet access and EDL credentials.
- Store: `obspec_utils.stores.AiohttpStore`, one instance per hostname, with `Authorization: Bearer <edl_token>` header.
- Many DAAC HTTPS gateways 302-redirect to presigned S3 URLs; aiohttp's default redirect behavior handles this.

## Sampling Strategy

For each collection with `skip_reason IS NULL`:

- **Stratified (preferred):** split the collection's temporal extent into 5 equal bins, sample one granule per bin (`count=1`, `temporal=(bin_start, bin_end)`). Store `stratified=True`.
- **Fallback:** if the collection lacks a temporal extent in CMR metadata, issue one `search_data(count=5)` call and accept whatever comes back. Store `stratified=False`.

Stratified sampling surfaces intra-collection heterogeneity: collections whose first granule virtualizes successfully but later ones fail (e.g., due to a mid-mission codec change or reprocessing campaign). These show up as `partial_pass` in the report.

## Error Handling

- **Per-attempt:** all exceptions caught and serialized. Timeouts produce `TimeoutError`-typed results. Auth failures produce `AuthUnavailable`-typed results.
- **No retries.** Flakiness surfaces naturally as `partial_pass` across the 5 stratified granules; retrying inside `attempt_one` would conflate transient failure with genuine lack of support and muddy the taxonomy.
- **Process-level:** SIGINT flushes the active Parquet shard and exits cleanly. The next `attempt` run resumes from where it stopped.

## Testing Strategy

### Unit (`tests/unit/`)

71+ tests across eight suites. Every module has mocked tests for its public API. Notable:

- `test_taxonomy.py` is table-driven with one case per hypothesized bucket; grows as the pilot reveals new patterns.
- `test_auth.py` covers both direct and external modes with `earthaccess` and `obspec_utils.stores.AiohttpStore` mocked. Does not exercise the real obstore S3 client or real EDL auth.
- `test_attempt.py::test_run_attempt_resumes` pre-populates a Parquet file and verifies the resume check skips already-attempted granules.
- `test_sample.py` covers both the stratified-bins branch and the no-temporal-extent fallback.

### Integration (`tests/integration/`, opt-in)

One smoke test that runs the full pipeline on 3 collections with real EDL credentials. Skipped cleanly when `~/.netrc` is absent. Not run by default in CI.

## Extensibility

### Refining the taxonomy

After a pilot run, read `output/report.md` — the "Top 20 Raw Errors in `OTHER`" section surfaces uncategorized errors with counts. For each recurring pattern:

- Add a new `Bucket` value in `taxonomy.py` if it represents a novel failure mode.
- Add a `(type_regex, message_regex, bucket)` rule at the appropriate position in `_RULES` (first match wins).
- Add a test case in `tests/unit/test_taxonomy.py`.
- Re-run `report` only; no need to re-run `attempt`.

### Adding a format family

- Add a value to `FormatFamily` in `formats.py`.
- Add declared-string and extension entries to `_DECLARED` and `_EXT`.
- Add a parser-dispatch branch in `attempt.py::dispatch_parser` if VirtualiZarr supports the format; otherwise it will be recorded as `NO_PARSER`.

### Adding an access mode

- Add a branch in `StoreCache.get_store` and whatever store construction is needed.
- Propagate the mode through the `--access` flag's `click.Choice`.
- `sample.py`'s `_extract_url` forwards the mode to `data_links(access=...)`.
- `_build_registry` already keys on scheme+host and should not need changes unless the new scheme requires a different registry shape.

## Known Limitations

- **Sequential only.** The run is single-process, single-threaded (apart from the per-attempt worker). Parallelism was deferred; at roughly 5 seconds per attempt and ~10,000 attempts total, the survey takes a workday. If that becomes a problem, the natural partitioning unit is the DAAC.
- **No schema migrations.** DB schema changes require deleting `output/survey.duckdb` and `output/results/` and re-running. Acceptable for a one-off survey.
- **Taxonomy drift.** Upstream error messages change. The regex-based classifier needs maintenance whenever VirtualiZarr or its dependencies evolve.
- **HTTPS redirect behavior is untested.** `external` mode depends on aiohttp following 302 redirects from DAAC gateways to presigned S3 URLs without dropping or misapplying the bearer header. Untested against the full DAAC matrix; edge-case failures will surface as bucketed errors.
- **VirtualiZarr parser coverage.** HDF4 and GeoTIFF collections land in `NO_PARSER` by design. A future VirtualiZarr release could close these gaps; the survey will pick that up automatically on the next run.

## Related Prior Art

- `titiler-cmr-compatibility` ran a structurally similar survey for tile-generation compatibility. Ported: EOSDIS provider filter, initial taxonomy buckets, collection-metadata fields to record (processing level, short name+version), format/extension mapping, Parquet-incremental-write pattern, per-granule timeout.
- Kept different: 5 stratified granules vs. their 1 random granule per collection (for heterogeneity detection); `open_virtual_dataset` as the test function vs. their `CMRBackend` tile render (different failure surface).
