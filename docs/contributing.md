# Contributing

Thanks for your interest in improving nasa-virtual-zarr-survey.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for Python environment management. No conda, no pip.

```bash
git clone https://github.com/NASA-IMPACT/nasa-virtual-zarr-survey.git
cd nasa-virtual-zarr-survey
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

The report page at `docs/results/index.md` embeds figures from `docs/results/figures/` (one `.html` and one `.png` per chart: `sankey`, `funnel`, `taxonomy_parse`, `taxonomy_dataset`, `taxonomy_datatree`, `by_daac`, `by_format`, `collections`). These are produced by `nasa-virtual-zarr-survey report`, which writes the figures as a side effect of rendering the Markdown.

Two ways to regenerate, depending on what changed:

1. From the committed digest (no survey re-run, no credentials needed). Fast path when you are only tweaking figure styling in `src/nasa_virtual_zarr_survey/figures.py` or the report template:

    ```bash
    uv run nasa-virtual-zarr-survey report \
      --from-data docs/results/summary.json \
      --out docs/results/index.md
    ```

2. From current survey state (requires a populated `output/survey.duckdb` and `output/results/`, i.e. you have run `discover`, `sample`, and `attempt` locally). This path also refreshes `docs/results/summary.json` if you pass `--export`:

    ```bash
    uv run nasa-virtual-zarr-survey report \
      --export docs/results/summary.json \
      --out docs/results/index.md
    ```

Commit the regenerated `docs/results/summary.json` if it changed, alongside your code change.

Documentation conventions:

- Write in plain prose, no em dashes.
- Use commas, colons, or parentheses for aside-style punctuation.
- Do not reference paths outside the project repo (they will break when the repo is published).

## End-to-end smoke test

To exercise the full pipeline from a clean slate and refresh the committed docs (`docs/results/index.md` and `docs/results/summary.json`):

```bash
uv run nasa-virtual-zarr-survey pilot --clean --top 5 --n-bins 3 --access external
```

`--clean` lists the paths it would delete and prompts for confirmation before wiping `output/survey.duckdb` and `output/results/`. The run hits live CMR and granule URLs, so it needs network and EDL credentials. `--clean` does not touch the granule cache, so cached bytes from prior runs are reused; combine with `--cache` to keep iteration fast:

```bash
uv run nasa-virtual-zarr-survey pilot --clean --cache --top 5 --n-bins 3 --access external
```

The pipeline itself is documented in the [design document](design/architecture.md).

## Common extension tasks

### Refining the taxonomy

After a pilot run, open `output/report.md` and scroll to "Top 20 Raw Errors in `OTHER`". Each recurring uncategorized error is a candidate for a new rule. Before promoting one to its own bucket, generate a `repro` script for the failing granule (`uv run nasa-virtual-zarr-survey repro --bucket OTHER --limit 3 --out reproductions/`) and run it: the structure dump tells you whether the error is genuinely a new failure mode or a more specific case of an existing bucket.

Then:

1. Add a new value to `Bucket` in `src/nasa_virtual_zarr_survey/taxonomy.py` if it represents a genuinely new failure mode.
2. Add a `(type_regex, message_regex, bucket)` tuple to `_RULES`. Order matters: first match wins.
3. Add a test case to `tests/unit/test_taxonomy.py` using the `@pytest.mark.parametrize` list.
4. Update the bucket table in `docs/design/taxonomy.md`.
5. Re-run `report`; no need to re-run `attempt`.

### Adding a format family

1. Add a value to `FormatFamily` in `src/nasa_virtual_zarr_survey/formats.py`.
2. Add declared-format strings and file extensions to `_DECLARED` and `_EXT`.
3. Add a dispatch branch in `attempt.dispatch_parser` if VirtualiZarr or an adjacent library supports parsing the format. If no parser exists, collections will record `error_type="NoParserAvailable"` naturally and no code change is required beyond `formats.py`.
4. Add a test case to `tests/unit/test_formats.py`.

### Adding an access mode

1. Add a branch in `auth.StoreCache.get_store` plus any store construction the new mode needs.
2. Expose it through the `--access` `click.Choice` in `__main__.py` for the `sample`, `attempt`, and `pilot` commands.
3. `sample._extract_url` already forwards the mode to `earthaccess.DataGranule.data_links(access=...)`, so usually no change is needed there.

## Inspecting the local cache

When `--cache` is enabled, fetched granule bytes are persisted under `~/.cache/nasa-virtual-zarr-survey/` (or whatever `--cache-dir` was set to). The layout is:

```
<cache_dir>/<scheme>/<host>/<sha256(url)>
```

`<scheme>` and `<host>` come from `urlparse(url)`, and the file name is the SHA-256 of the granule's data URL exactly as stored in `granules.data_url`. Listing a single bucket or host shows every cached entry for that target:

```bash
ls -lh ~/.cache/nasa-virtual-zarr-survey/s3/podaac-ops-cumulus-protected/
```

To find the cache path for a specific granule, look up its URL in the DuckDB and hash it:

```bash
url=$(duckdb output/survey.duckdb -noheader -list -s \
  "SELECT data_url FROM granules WHERE granule_concept_id = 'G123-XYZ';")

python3 -c "
import sys, hashlib
from urllib.parse import urlparse
url = sys.argv[1]
p = urlparse(url)
print(f'~/.cache/nasa-virtual-zarr-survey/{p.scheme}/{p.netloc}/{hashlib.sha256(url.encode()).hexdigest()}')
" "$url"
```

To go the other direction, given a cache file name, find the granule by hashing every URL in the `granules` table and matching:

```bash
duckdb output/survey.duckdb -noheader -list -s \
  "SELECT granule_concept_id, data_url FROM granules;" \
  | python3 -c "
import sys, hashlib
target = sys.argv[1]
for line in sys.stdin:
    gid, _, url = line.partition('|')
    if hashlib.sha256(url.strip().encode()).hexdigest() == target:
        print(gid, url.strip())
" "<sha256-from-filename>"
```

To clear the cache, delete the directory: `rm -rf ~/.cache/nasa-virtual-zarr-survey/`. The total size is bounded by the `--cache-max-size` cap; once exceeded, the survey logs a warning once per process and falls through to direct fetches without caching new granules. Existing cache entries continue to serve reads.

See the "Local granule cache" section of `docs/design/architecture.md` for the full layout and trade-offs.

## Code style

- Small, focused files. Each module in `src/nasa_virtual_zarr_survey/` has one clear responsibility.
- Google-style docstrings on public classes and functions. Private helpers (leading underscore) do not require docstrings unless the behavior is non-obvious.
- Prefer explicit keyword arguments in public APIs, especially across module boundaries.
- No em dashes in text, docstrings, or commit messages. Commas or colons work fine.
- Commit messages use short imperative subject lines. The project starts each message with a Conventional Commits prefix when it fits (`feat:`, `fix:`, `test:`, `chore:`, `docs:`), but this is not strictly enforced.

## Reporting issues

Please include:

- Command you ran, including all flags.
- Expected behavior versus observed behavior.
- Relevant section of `output/report.md` if the issue was discovered in the pipeline output.
- For parse or dataset failures you believe are misclassified, the raw `error_type` and `error_message` from `results.parquet`.

You can query the Parquet log directly:

```bash
uv run python -c "
import duckdb
print(duckdb.sql('''
    SELECT parse_error_type, parse_error_message, count(*)
    FROM read_parquet(\"output/results/**/*.parquet\", union_by_name=true, hive_partitioning=true)
    WHERE parse_success = false
    GROUP BY 1, 2
    ORDER BY count(*) DESC
    LIMIT 20
'''))
"
```
