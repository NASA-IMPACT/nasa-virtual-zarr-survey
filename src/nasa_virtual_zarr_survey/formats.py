"""Map CMR-declared formats and file extensions to array-like format families."""

from __future__ import annotations

from enum import StrEnum
from pathlib import PurePosixPath
from urllib.parse import urlparse


class FormatFamily(StrEnum):
    """Array-like format families recognized by the survey.

    The string value is what is stored in the DuckDB `collections.format_family`
    column and used as a grouping key in reports. Collections whose declared
    format maps to one of these families are attempted during Phases 3-4; others
    are filtered out with `skip_reason="non_array_format"`.
    """

    NETCDF4 = "NetCDF4"
    NETCDF3 = "NetCDF3"
    HDF5 = "HDF5"
    HDF4 = "HDF4"
    ZARR = "Zarr"
    GEOTIFF = "GeoTIFF"
    FITS = "FITS"
    DMRPP = "DMRPP"


_DECLARED: dict[str, FormatFamily] = {
    s.lower(): family
    for family, strings in {
        FormatFamily.NETCDF4: [
            "NetCDF",
            "NetCDF-4",
            "netCDF-4",
            "NetCDF4",
            "netCDF4",
            "netCDF-4 classic",
        ],
        FormatFamily.NETCDF3: ["NetCDF-3", "netCDF-3", "NetCDF3", "netCDF classic"],
        FormatFamily.HDF5: ["HDF5", "HDF-EOS5", "HDF5-EOS"],
        FormatFamily.HDF4: ["HDF", "HDF4", "HDF-EOS", "HDF-EOS2"],
        FormatFamily.ZARR: ["Zarr", "zarr"],
        FormatFamily.GEOTIFF: ["COG", "GeoTIFF", "Cloud-Optimized GeoTIFF"],
        FormatFamily.FITS: ["FITS"],
        FormatFamily.DMRPP: ["DMR++", "dmr++", "DMRPP"],
    }.items()
    for s in strings
}


_EXT: dict[str, FormatFamily] = {
    ".nc": FormatFamily.NETCDF4,
    ".nc4": FormatFamily.NETCDF4,
    ".h5": FormatFamily.HDF5,
    ".hdf5": FormatFamily.HDF5,
    ".he5": FormatFamily.HDF5,
    ".hdf": FormatFamily.HDF4,
    ".zarr": FormatFamily.ZARR,
    ".tif": FormatFamily.GEOTIFF,
    ".tiff": FormatFamily.GEOTIFF,
    ".cog": FormatFamily.GEOTIFF,
    ".fits": FormatFamily.FITS,
    ".fit": FormatFamily.FITS,
    ".dmrpp": FormatFamily.DMRPP,
}


def classify_format(declared: str | None, url: str | None) -> FormatFamily | None:
    """Return the array-like family for a CMR-declared format or file URL, else None."""
    if declared:
        hit = _DECLARED.get(declared.strip().lower())
        if hit is not None:
            return hit
    if url:
        path = PurePosixPath(urlparse(url).path)
        # strip ".zarr" off group-like URLs too
        for part in [
            path.suffix.lower(),
            *(f".{p}" for p in path.name.lower().split(".")[1:]),
        ]:
            if part in _EXT:
                return _EXT[part]
    return None
