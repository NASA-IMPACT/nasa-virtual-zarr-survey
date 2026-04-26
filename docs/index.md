# nasa-virtual-zarr-survey

Surveys cloud-hosted NASA CMR collections for VirtualiZarr compatibility. The pipeline runs as five phases:

1. **Discover** (Phase 1): enumerate CMR collections into DuckDB.
2. **Sample** (Phase 2): pick N granules per collection, stratified across its temporal extent.
3. **Parsability** (Phase 3): the VirtualiZarr parser can produce a `ManifestStore` from a granule URL.
4. **Datasetability / Datatreeability** (Phase 4a / 4b): the `ManifestStore` can be converted to an `xarray.Dataset` (4a) or `xarray.DataTree` (4b). 4b runs in parallel with 4a; it captures hierarchical files that fail 4a with `CONFLICTING_DIM_SIZES`.
5. **Cubability** (Phase 5): the per-granule datasets can be combined into a coherent virtual store. Gated on Phase 4a `all_pass` (tree-only collections are not yet cubable).

Failures in each phase are categorized into an empirically-derived taxonomy so VirtualiZarr maintainers and NASA DAAC operators can prioritize gaps. New to the terms (`granule`, `DAAC`, `ManifestStore`, `cubability`)? See the [glossary](glossary.md). For the bucket meanings, see [the taxonomy reference](design/taxonomy.md).

## Getting started

```bash
uv sync
```

Requires Earthdata Login credentials in `~/.netrc`. See NASA's [Earthdata Login setup guide](https://urs.earthdata.nasa.gov/) if you don't already have an account; the same `.netrc` entry powers both `--access external` (HTTPS + bearer token) and `--access direct` (S3 credentials minted via EDL).

### Pilot run

Start small. The pilot runs discover, sample, attempt, report on a bounded set of collections so you can review raw errors and refine the failure taxonomy before committing to a full survey:

```bash
uv run nasa-virtual-zarr-survey pilot --top 20 --n-bins 2 --access external
```

### Per-phase commands

The pilot is a convenience wrapper; the full pipeline is also available one phase at a time:

```bash
uv run nasa-virtual-zarr-survey discover --top 200
uv run nasa-virtual-zarr-survey sample --n-bins 5
uv run nasa-virtual-zarr-survey attempt --access external
uv run nasa-virtual-zarr-survey report
```

`discover` and `sample` are idempotent; `attempt` is resumable (it skips `(collection, granule)` pairs already present in the Parquet log); `report` is cheap and side-effect-free, so you can re-run it after refining the taxonomy without re-fetching anything.

## Run modes in detail

### Scope: `--top` vs `--top-per-provider` vs default

Without a scope flag, `discover` enumerates **all** EOSDIS cloud-hosted collections (thousands). For most iteration that is too much:

- `--top N`: surveys the top-N most-used collections across EOSDIS providers, ranked by CMR's `usage_score`.
- `--top-per-provider N`: takes N per provider, so smaller DAACs are not drowned out by larger ones.

Pick one or the other; the default (no scope flag) is reserved for a full survey.

### Granule depth: `--n-bins`

`--n-bins N` samples N granules per collection, stratified evenly across the collection's temporal extent. Default is 5. Use 2 for fast iteration; raise it to surface flakiness or long-tail per-granule heterogeneity (which shows up as `partial_pass` verdicts).

### Access mode: `--access external` vs `--access direct`

NASA's EOSDIS S3 buckets live in `us-west-2` and don't allow public direct-S3 reads from outside that region.

- `--access external` (default for outside AWS): HTTPS URLs signed with an Earthdata Login bearer token. Works from anywhere with an `~/.netrc` EDL entry, but every byte goes through HTTPS rather than the cheaper S3 path.
- `--access direct`: temporary S3 credentials minted via EDL's cloud-auth endpoint. Requires that you're running on `us-west-2` compute. Faster and avoids HTTPS gateway costs, but a `403 Forbidden` (taxonomy bucket `FORBIDDEN`) is the typical failure if you try this from outside the region.

If you change access mode between runs, `sample` re-extracts the granule URLs (the URL format differs between the two), so a `discover` re-run is not needed.

### Cloud OPeNDAP / DMR++ sidecars

`discover` records `collections.has_cloud_opendap` (true when the collection is associated with the cloud-Hyrax UMM-S record); `sample` then writes `granules.dmrpp_granule_url = https_url + ".dmrpp"` for each sampled granule of those collections. The constructed URL is the input `DMRPPParser` reads — useful as a fallback when the underlying HDF5 parse fails. Use `--verify-dmrpp` on `sample` (off by default; one HEAD per granule) to confirm each sidecar actually exists rather than trusting the UMM-S association alone.

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
uv run nasa-virtual-zarr-survey discover --top 50 --list array --dry-run

# audit which collections were filtered out and why
uv run nasa-virtual-zarr-survey discover --top 50 --list skipped --dry-run

# full picture, with skip_reason populated for the filtered rows
uv run nasa-virtual-zarr-survey discover --top 50 --list all --dry-run
```

The table includes a plain Earthdata Search URL per row (`https://search.earthdata.nasa.gov/search?q=<concept_id>`); modern terminals auto-linkify it for Cmd/Ctrl-click. For a UMM-JSON dump, use `probe <concept_id>` — it prints both the search and CMR concept URLs.

`--list` works with or without `--dry-run`; in persisted mode the DB is populated as a side effect, useful when the listing confirms the selection.

### Dry run

To see what would be fetched without writing to the DB:

```bash
uv run nasa-virtual-zarr-survey discover --top 20 --dry-run
```

### Caching granule bytes

Iterating on the taxonomy or report code without re-downloading granules is much faster with a local cache. Add `--cache` to any command that fetches granules to persist fetched bytes under `~/.cache/nasa-virtual-zarr-survey/`:

```bash
uv run nasa-virtual-zarr-survey pilot --cache --top 5 --n-bins 3 --access external
```

Override the location with `--cache-dir` or `NASA_VZ_SURVEY_CACHE_DIR`, and bound total size with `--cache-max-size`. See the contributing guide for cache layout and inspection tips.

## Reproducing a single failure

After a `report` run, the most common next step is investigating one specific failure. The `repro` subcommand emits a self-contained Python script that reproduces the failing operation against the same URL, parser, and kwargs the survey used:

```bash
# emit one repro for a specific collection
uv run nasa-virtual-zarr-survey repro C1996881146-POCLOUD --out reproductions/

# emit up to 3 repros for a failure bucket (great for triage)
uv run nasa-virtual-zarr-survey repro --bucket UNDEFINED_FILL_VALUE --limit 3 --out reproductions/

# emit only failures from a specific phase
uv run nasa-virtual-zarr-survey repro --bucket CONFLICTING_DIM_SIZES --phase dataset --out reproductions/
```

Each generated script attempts the failing parser / dataset call against the same URL the survey used:

```bash
uv run python reproductions/repro_G123456789-POCLOUD.py
uv run python reproductions/repro_G123456789-POCLOUD.py --cache   # reuse fetched bytes locally
```

By default the renderer bakes any matching collection override into the script; pass `--no-overrides` to render an unconfigured run (useful when investigating a regression). The script also doubles as a working starting point for non-debugging virtualization workflows — edit the parser/dataset kwargs (or strip the failure-context docstring) and treat it as a runnable seed.

For a structural dump (group tree, dtypes, chunks, codecs, fill values) — or to investigate a collection that `repro` cannot help with, e.g. one skipped at discover time with `format_unknown` — use `probe`.

## Probing a collection or granule

`probe` is the diagnostic counterpart to `repro`. Where `repro` reproduces a failure the survey already observed, `probe` investigates any concept ID — most importantly collections that were skipped at discover time (no Parquet failures to reproduce):

```bash
# write a probe script for a collection
uv run nasa-virtual-zarr-survey probe C1214470488-ASF --out probes/

# write a probe for a specific granule
uv run nasa-virtual-zarr-survey probe G1245678901-ASF --out probes/
```

The generated script logs in via `earthaccess`, dumps the collection / granule UMM-JSON and both `direct` and `external` data links, and (when format can be sniffed from the URL extension) calls `inspect_url` for a structural dump. `probe` prefers the local survey DB and falls back to one or two CMR calls when the concept ID is absent — so it works against a fresh checkout as well as against a populated `output/survey.duckdb`.

If `repro CONCEPT_ID` cannot find any failures because the collection was skipped or never sampled, the error message points you at the right `probe` invocation.

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

After editing the file, validate it before running anything that reads it:

```bash
uv run nasa-virtual-zarr-survey validate-overrides
```

The check enforces concept-ID format, allowed sub-keys, kwarg names against each parser's signature, and contradictory combinations (e.g. `skip_dataset = true` plus `dataset = {...}`).

The full debug loop is: naive `attempt` records a failure, `repro` to investigate, edit `collection_overrides.toml`, `validate-overrides` to confirm, then re-run `attempt` for that collection. The next `report` will mark the collection as `override_applied = true`. See [the design doc](design/architecture.md#per-collection-overrides) for the full schema and validation rules.

## Tracking compatibility over time

The pipeline above is one point in time. To track how VirtualiZarr's compatibility against NASA data evolves across releases — and to evaluate unreleased branches against the same fixed sample — the survey supports *snapshots*.

A snapshot is one re-run of `attempt` + `report` against `config/locked_sample.json` (a committed JSON enumeration of (collection, granule) pairs) under a date-pinned dependency stack. Each snapshot writes a `*.summary.json` digest to `docs/results/history/`; the `history` subcommand renders all of them as a [Coverage over time](results/history.md) page with funnel-over-time and bucket-trend charts.

Two flavors:

- **Release** snapshots pin to a single date (`[tool.uv] exclude-newer`).
- **Preview** snapshots pin to a date plus one or more `[tool.uv.sources]` git overrides — used to evaluate unreleased branches.

Typical workflow once the locked sample is set up (see the contributing guide):

```bash
# release: pin pyproject's exclude-newer date, lock, snapshot.
uv lock
uv run nasa-virtual-zarr-survey snapshot

# preview: add a git override to [tool.uv.sources], lock, snapshot with a label.
uv lock
uv run nasa-virtual-zarr-survey snapshot --label variable-chunking

# render the page after committing new digests.
uv run nasa-virtual-zarr-survey history
```

See [Publishing a snapshot](contributing.md#publishing-a-snapshot) in the contributing guide for the end-to-end walk-through, including how to build the locked sample.

## Querying the raw data

The Parquet log at `output/results/` is the canonical per-attempt record (one row per granule per phase). DuckDB can read it directly and is the fastest way to answer questions like "which DAACs hit `UNSUPPORTED_CODEC` most?":

```bash
uv run python -c "
import duckdb
print(duckdb.sql('''
    SELECT daac, parse_error_type, count(*) AS n
    FROM read_parquet(\"output/results/**/*.parquet\", union_by_name=true, hive_partitioning=true)
    WHERE parse_success = false
    GROUP BY 1, 2
    ORDER BY n DESC
    LIMIT 20
'''))
"
```

`output/survey.duckdb` carries the discover/sample state (collections, granules, run metadata). The Parquet log carries every per-attempt result. Together they're enough to recompute the report from scratch with `report --from-data` skipped.

## Architecture

State is persisted in a DuckDB database (`output/survey.duckdb`) for checkpoint data, and in DAAC-partitioned Parquet shards (`output/results/`) for the append-only per-attempt log. Both stages are resumable: `discover` and `sample` are idempotent, `attempt` skips already-recorded `(collection, granule)` pairs.

For the full design, including the override mechanism, the repro renderer, and per-format inspectors, see the [design document](design/architecture.md).
