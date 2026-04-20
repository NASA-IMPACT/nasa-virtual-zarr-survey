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

See the design spec at `docs/superpowers/specs/2026-04-20-nasa-virtual-zarr-survey-design.md`.
