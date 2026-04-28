# Contributing

Thanks for your interest in improving virtual-zarr-coverage.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for Python environment management. No conda, no pip.

```bash
git clone https://github.com/NASA-IMPACT/virtual-zarr-coverage.git
cd virtual-zarr-coverage
uv sync
```

`uv sync` installs the runtime dependencies plus the `dev` dependency group (pytest, ruff, mypy). Add `--group docs` if you plan to build the documentation locally.

For live survey runs you will also need Earthdata Login credentials in `~/.netrc`. The integration tests skip cleanly without them.

## Running tests

```bash
uv run pytest tests/unit/ -v           # fast, always runs (no network)
uv run pytest -m integration           # opt-in; requires ~/.netrc
uv run pytest tests/ -v                # everything
```

The unit test suite is the primary gate. Integration tests hit real NASA endpoints and should only be run deliberately.

## Type checking and linting

```bash
uv run mypy src/
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

## Documentation

```bash
uv sync --group docs
uv run --group docs mkdocs serve      # live preview on http://localhost:8000
uv run --group docs mkdocs build      # writes to site/
```

The API reference pages are generated from source via mkdocstrings, so adding a public class or function automatically requires a matching entry in the relevant `docs/api/*.md` file for it to show up.

### Regenerating report figures

The report page at `docs/results/index.md` embeds figures from `docs/results/figures/` (one `.html` and one `.png` per chart: `sankey`, `funnel`, `taxonomy_parse`, `taxonomy_dataset`, `taxonomy_datatree`, `by_daac`, `by_format`, `collections`). These are produced by `vzc render`, which writes the figures as a side effect of rendering the Markdown.

Two ways to regenerate, depending on what changed:

1. From the committed digest (no survey re-run, no credentials needed). Fast path when you are only tweaking figure styling in `src/vzc/render/_figures.py` or the report template:

    ```bash
    uv run vzc render --from-data docs/results/summary.json
    ```

2. From current survey state (requires a populated `output/state.json` and `output/results/`, i.e. you have run `discover`, `sample`, and `attempt` locally):

    ```bash
    uv run vzc render
    ```

`docs/results/summary.json` is the committed digest the first path reads from; it is refreshed only by the `run` (snapshot) subcommand. Commit it alongside the regenerated `docs/results/index.md` and `docs/results/figures/` when a snapshot run produces a new digest.

Documentation conventions:

- Write in plain prose, no em dashes.
- Use commas, colons, or parentheses for aside-style punctuation.
- Do not reference paths outside the project repo (they will break when the repo is published).

## End-to-end smoke test

To exercise the full pipeline from a clean slate and refresh the committed docs (`docs/results/index.md` and `docs/results/summary.json`):

```bash
rm -rf output/state.json output/results/
uv run vzc discover --top 5
uv run vzc sample --n-bins 3
uv run vzc prefetch --cache-max-size 5GB
uv run vzc attempt --access external
uv run vzc render
```

The run hits live CMR and granule URLs, so it needs network and EDL credentials. The cache (under `~/.cache/nasa-virtual-zarr-survey/`) is not touched by `rm -rf output/...`, so cached bytes from prior runs are reused on the second invocation.

The pipeline itself is documented in the [design document](design/architecture.md).

## Common extension tasks

### Refining the taxonomy

After a survey run, open `docs/results/index.md` and scroll to "Top 20 Raw Errors in `OTHER`". Each recurring uncategorized error is a candidate for a new rule. Before promoting one to its own bucket, pick a failing concept ID and emit an investigation script (`uv run vzc investigate <CONCEPT_ID>`); run it to see whether the error is genuinely a new failure mode or a more specific case of an existing bucket.

Then:

1. Add a new value to `Bucket` in `src/vzc/core/taxonomy.py` if it represents a genuinely new failure mode.
2. Add a `(type_regex, message_regex, bucket)` tuple to `_RULES`. Order matters: first match wins.
3. Add a test case to `tests/unit/test_taxonomy.py` using the `@pytest.mark.parametrize` list.
4. Update the bucket table in `docs/design/taxonomy.md`.
5. Re-run `render`; no need to re-run `attempt`.

### Adding a format family

1. Add a value to `FormatFamily` in `src/vzc/core/formats.py`.
2. Add declared-format strings and file extensions to `_DECLARED` and `_EXT`.
3. Add a dispatch branch in `pipeline._attempt.dispatch_parser` if VirtualiZarr or an adjacent library supports parsing the format. If no parser exists, collections will record `error_type="NoParserAvailable"` naturally and no code change is required beyond `formats.py`.
4. Add a test case to `tests/unit/test_formats.py`.

### Adding an access mode

1. Add a branch in `pipeline._stores.StoreCache.get_store` plus any store construction the new mode needs.
2. Expose it through the `--access` `click.Choice` in the `attempt`, `investigate`, and `run` subcommand modules under `cli/commands/`.
3. `cmr._sample._extract_urls` already forwards the mode to `earthaccess.DataGranule.data_links(access=...)`, so usually no change is needed there.

## Inspecting the local cache

`prefetch` writes fetched granule bytes under `~/.cache/nasa-virtual-zarr-survey/` (override via `NASA_VZ_SURVEY_CACHE_DIR`). The layout is:

```
<cache_dir>/<scheme>/<host>/<sha256(url)>
```

`<scheme>` and `<host>` come from `urlparse(url)`, and the file name is the SHA-256 of the granule's `https_url` (prefetch is HTTPS-only). Listing a host shows every cached entry for that target:

```bash
ls -lh ~/.cache/nasa-virtual-zarr-survey/https/data.podaac.earthdata.nasa.gov/
```

To find the cache path for a specific granule, look up its URL in `output/state.json` and hash it:

```bash
python3 -c "
import json, hashlib, sys
from urllib.parse import urlparse

gid = sys.argv[1]
state = json.load(open('output/state.json'))
g = next(g for g in state['granules'] if g['granule_concept_id'] == gid)
url = g['https_url'] or g['s3_url']
p = urlparse(url)
print(f'~/.cache/nasa-virtual-zarr-survey/{p.scheme}/{p.netloc}/{hashlib.sha256(url.encode()).hexdigest()}')
" G123-XYZ
```

To go the other direction, given a cache file name, find the granule by hashing every URL in `state.json` and matching:

```bash
python3 -c "
import json, hashlib, sys

target = sys.argv[1]
state = json.load(open('output/state.json'))
for g in state['granules']:
    for url in (g.get('s3_url'), g.get('https_url')):
        if url and hashlib.sha256(url.encode()).hexdigest() == target:
            print(g['granule_concept_id'], url)
" "<sha256-from-filename>"
```

To clear the cache, delete the directory: `rm -rf ~/.cache/nasa-virtual-zarr-survey/`. The total size is bounded by the `--cache-max-size` cap on `prefetch`; the cap is checked at collection boundaries, so the collection that pushes past the cap finishes writing all its granules before prefetch stops.

See the "Local granule cache" section of `docs/design/architecture.md` for the full layout and trade-offs.

## Publishing a snapshot

A *snapshot* is one re-run of the survey under a date-pinned dependency stack. Each snapshot writes a `*.summary.json` digest under `docs/results/history/` and appears on the [Coverage over time](results/history.md) page when re-rendered.

Two flavors:

- **Release** — pin to a single date. The digest captures resolved package version strings; if you need byte-exact reproduction later, commit `uv.lock` yourself.
- **Preview** — same date pin plus one or more `git+...` overrides. Used to evaluate unreleased branches (e.g. a VirtualiZarr PR) against the same fixed sample.

The rest of this section walks through (1) building the locked sample once, (2) running release snapshots, (3) running preview snapshots, and (4) re-rendering the history page.

### Step 1: Build the locked sample (one-time)

Snapshots compare *the same set* of (collection, granule) pairs across runs, so this set is committed once as `config/locked_sample.json`. Rebuild only when you intentionally want to change what's evaluated.

#### 1a. Discover collections

`discover` enumerates cloud-hosted CMR collections and writes them to `output/state.json`. `--top N` picks the N most-used collections by CMR `usage_score`; 100 is a good starting point.

```bash
uv run vzc discover --top 100
```

Preview before committing to a run:

```bash
uv run vzc discover --top 100 --list array --dry-run
```

The `--list array` view shows the collections that would feed `sample` (the array-like ones); `--list skipped` shows what was filtered out and why; `--list all` shows both. See the [usage docs](index.md) for the full set of `--list` modes.

#### 1b. Sample granules per collection

`sample` picks 5 granules per collection (the default), stratified across positional offsets in CMR's `revision_date` ordering, so coverage spans the collection's reprocessing history (which is where codec/format changes appear).

```bash
uv run vzc sample --access external
```

`--access external` (HTTPS + EDL bearer token) is the right default for shared snapshots: it works from anywhere with `~/.netrc`. `--access direct` (S3 credentials) is faster but only works from `us-west-2` compute; using it for a committed sample would lock everyone else out.

#### 1c. Freeze the sample

The locked sample uses the same JSON shape as `output/state.json` (see the [Data model section of the design doc](design/architecture.md#data-model)). Trim the live state down to what's needed for reproducibility — typically the small fields per granule, dropping `umm_json`:

```bash
uv run python - <<'EOF'
import json
src = json.load(open("output/state.json"))
locked = {
    "schema_version": src["schema_version"],
    "run_meta": src["run_meta"],
    "collections": [
        {k: v for k, v in c.items() if k != "umm_json"}
        for c in src["collections"]
    ],
    "granules": [
        {k: v for k, v in g.items() if k != "umm_json"}
        for g in src["granules"]
    ],
}
with open("config/locked_sample.json", "w") as f:
    json.dump(locked, f, indent=2)
EOF
git add config/locked_sample.json
git commit -m "Lock survey sample"
```

Both `s3_url` and `https_url` are recorded per granule so future snapshots can run under either access mode against the same sample.

#### 1d. Validate overrides (optional but recommended)

Before snapshotting, sanity-check that `config/collection_overrides.toml` resolves cleanly. Validation runs at the start of every `attempt`, so the cheap dry-run check is a one-granule attempt:

```bash
uv run vzc attempt --access external --shard-size 1
# Aborts at startup with the first override error if any; otherwise runs.
# Use --skip-override-validation to suppress the startup check entirely.
```

### Step 2: Run a release snapshot

The snapshot date is read from `[tool.uv] exclude-newer` in `pyproject.toml`. Set it to the date you want to evaluate, re-lock, then run:

```toml
# pyproject.toml
[tool.uv]
exclude-newer = "2026-02-15"
```

```bash
uv lock
uv run vzc run
git add docs/results/history/2026-02-15.summary.json
git commit -m "Snapshot 2026-02-15"
```

Alternative paths if you'd rather not edit `pyproject.toml`:

- One-shot: `uv lock --exclude-newer 2026-02-15 && uv run vzc run --snapshot-date 2026-02-15`.
- Override pyproject's date on the CLI: `uv run vzc run --snapshot-date 2026-02-15`.

`run --access external` (the default) reads from the cache at `~/.cache/nasa-virtual-zarr-survey/` (override via `$NASA_VZ_SURVEY_CACHE_DIR`). Run `prefetch` once to populate the cache; subsequent snapshots reuse the cached bytes. `run --access direct` skips the cache entirely.

### Step 3: Run a preview snapshot

Previews are auto-detected from `[tool.uv.sources]` git entries. The typical workflow:

```toml
# pyproject.toml
[tool.uv]
exclude-newer = "2026-04-26"

[tool.uv.sources]
virtualizarr = { git = "https://github.com/zarr-developers/VirtualiZarr", rev = "abc123de" }
```

```bash
uv lock
uv run vzc run \
    --label variable-chunking \
    --description "Coordinated VirtualiZarr branch test"
git add docs/results/history/2026-04-26-variable-chunking.summary.json
git commit -m "Preview snapshot: variable-chunking"
```

`rev` must be a hex SHA (7-40 chars). Branch names and tags are rejected because they're not reproducible.

### Step 4: Re-render the history page

After committing a new digest, refresh the rendered page:

```bash
uv run vzc render --history
git add docs/results/history.md docs/results/history/figures
git commit -m "Re-render history page"
```

Feature markers on the funnel-over-time chart are sourced from `config/feature_introductions.toml`. Add an entry when you ship a feature you want annotated:

```toml
[has_datatree]
phases = ["datatree"]
first_in_vz = "2.0.0"
introduced = "2026-03-15"
description = "ManifestStore.to_virtual_datatree() lands"
```

## Code style

- Small, focused files. Each module in `src/vzc/` has one clear responsibility.
- Google-style docstrings on public classes and functions. Private helpers (leading underscore) do not require docstrings unless the behavior is non-obvious.
- Prefer explicit keyword arguments in public APIs, especially across module boundaries.
- No em dashes in text, docstrings, or commit messages. Commas or colons work fine.
- Commit messages use short imperative subject lines. The project starts each message with a Conventional Commits prefix when it fits (`feat:`, `fix:`, `test:`, `chore:`, `docs:`), but this is not strictly enforced.

## Reporting issues

Please include:

- Command you ran, including all flags.
- Expected behavior versus observed behavior.
- Relevant section of `docs/results/index.md` if the issue was discovered in the rendered report.
- For parse or dataset failures you believe are misclassified, the raw `error_type` and `error_message` from `results.parquet`.

You can query the Parquet log directly with pyarrow:

```bash
uv run python -c "
from collections import Counter
import pyarrow.parquet as pq
from pathlib import Path

counts: Counter = Counter()
for shard in Path('output/results').rglob('*.parquet'):
    t = pq.read_table(
        shard, columns=['parse_success', 'parse_error_type', 'parse_error_message']
    )
    for ok, et, em in zip(*(t[c].to_pylist() for c in t.column_names)):
        if not ok and et:
            counts[(et, em)] += 1
for (et, em), n in counts.most_common(20):
    print(f'{n:5d}  {et}: {(em or \"\")[:80]}')
"
```
