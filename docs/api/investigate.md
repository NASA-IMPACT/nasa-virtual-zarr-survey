# investigate

Emit a runnable Python script for one CMR collection or granule.
Replaces the previous `probe` (native exploration) and `repro`
(survey-path reproduction) commands with a single tool gated by `--mode`.

`--mode virtual` (default) emits a script that reproduces the survey's
VirtualiZarr code path (`parser → manifest_store → to_virtual_dataset/datatree`)
— useful for stepping through a parser- or xarray-level failure with the
same overrides the survey runs under.

`--mode native` emits an exploration script using the format-appropriate
library (`h5py` for HDF5/NetCDF4, `netCDF4` for NetCDF3, `astropy` for
FITS, `zarr` for Zarr, `tifffile` for GeoTIFF) — useful for format triage
independent of any virtualization, or when investigating a collection
skipped at discover time (`skip_reason='format_unknown'`).

Pipe the result to `uv run python -` to execute now, or write to a file
for later iteration.

::: vzc.investigate
    handler: python
