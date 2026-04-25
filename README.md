# nasa-virtual-zarr-survey

Surveys cloud-hosted NASA CMR collections for VirtualiZarr compatibility.

## Usage

```bash
uv sync
uv run nasa-virtual-zarr-survey pilot --sample 50
# review output/results/ by hand, refine src/nasa_virtual_zarr_survey/taxonomy.py
uv run nasa-virtual-zarr-survey discover
uv run nasa-virtual-zarr-survey sample
uv run nasa-virtual-zarr-survey attempt
uv run nasa-virtual-zarr-survey report
```

Requires Earthdata Login credentials in `~/.netrc`.

### End-to-end smoke test

To exercise the full pipeline from a clean slate and refresh the docs (`docs/results/index.md` + `docs/results/summary.json`):

```bash
uv run nasa-virtual-zarr-survey pilot --clean --top 5 --n-bins 3 --access external
```

`--clean` lists the paths it would delete and prompts for confirmation before wiping `output/survey.duckdb` and `output/results/`. The run hits live CMR and granule URLs, so it needs network and EDL credentials.

For repeat runs (e.g., iterating on the taxonomy or report code without re-downloading granules), add `--cache` to persist fetched bytes under `~/.cache/nasa-virtual-zarr-survey/` (override with `--cache-dir` or `NASA_VZ_SURVEY_CACHE_DIR`):

```bash
uv run nasa-virtual-zarr-survey pilot --clean --cache --top 5 --n-bins 3 --access external
```

`--clean` does not touch the granule cache, so cached bytes from prior runs are reused across `--clean` invocations.
