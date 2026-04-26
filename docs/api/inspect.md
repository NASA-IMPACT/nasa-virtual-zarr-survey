# Inspect

Per-format structural inspectors that dump a granule's metadata. Called by generated `probe` scripts (and by hand from operator code) to turn each granule into a datasheet. Inspectors share the same `obstore` store as the parser, so there's no separate auth path.

Dispatches per `FormatFamily`: HDF5/NetCDF4 via `h5py`, Zarr via `zarr-python`, NetCDF3 via `netCDF4`, GeoTIFF via `tifffile`, DMR++ via XML parsing, FITS via `astropy.io.fits`. Inspectors emit human-readable text plus a fenced JSON block (`<<<INSPECT_JSON_BEGIN>>>` / `<<<INSPECT_JSON_END>>>`) for downstream tooling.

::: nasa_virtual_zarr_survey.inspect.inspect_url
    handler: python

::: nasa_virtual_zarr_survey.inspect.register
    handler: python
