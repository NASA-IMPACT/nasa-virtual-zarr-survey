# Contributing

Thanks for your interest in improving nasa-virtual-zarr-survey.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for Python environment management. No conda, no pip.

```bash
git clone https://github.com/developmentseed/nasa-virtual-zarr-survey.git
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

## Pipeline overview

See the [design document](design/architecture.md) for the full walk-through. Short version:

1. `discover` enumerates CMR collections into DuckDB.
2. `sample` picks up to N granules per collection, stratified across each collection's temporal extent.
3. `attempt` runs Phase 3 (Parsability) and Phase 4 (Datasetability) on each granule, writing results to partitioned Parquet.
4. `report` rolls up verdicts, runs Phase 5 (Virtual Store Feasibility / Cubability) on fingerprints, applies the taxonomy classifier, and renders `report.md`.

## Common extension tasks

### Refining the taxonomy

After a pilot run, open `output/report.md` and scroll to "Top 20 Raw Errors in `OTHER`". Each recurring uncategorized error is a candidate for a new rule:

1. Add a new value to `Bucket` in `src/nasa_virtual_zarr_survey/taxonomy.py` if it represents a genuinely new failure mode.
2. Add a `(type_regex, message_regex, bucket)` tuple to `_RULES`. Order matters: first match wins.
3. Add a test case to `tests/unit/test_taxonomy.py` using the `@pytest.mark.parametrize` list.
4. Re-run `report`; no need to re-run `attempt`.

### Adding a format family

1. Add a value to `FormatFamily` in `src/nasa_virtual_zarr_survey/formats.py`.
2. Add declared-format strings and file extensions to `_DECLARED` and `_EXT`.
3. Add a dispatch branch in `attempt.dispatch_parser` if VirtualiZarr or an adjacent library supports parsing the format. If no parser exists, collections will record `error_type="NoParserAvailable"` naturally and no code change is required beyond `formats.py`.
4. Add a test case to `tests/unit/test_formats.py`.

### Adding an access mode

1. Add a branch in `auth.StoreCache.get_store` plus any store construction the new mode needs.
2. Expose it through the `--access` `click.Choice` in `__main__.py` for the `sample`, `attempt`, and `pilot` commands.
3. `sample._extract_url` already forwards the mode to `earthaccess.DataGranule.data_links(access=...)`, so usually no change is needed there.

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
