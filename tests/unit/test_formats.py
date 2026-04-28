import pytest

from vzc.core.formats import FormatFamily, classify_format


@pytest.mark.parametrize(
    "declared,url,expected",
    [
        ("NetCDF-4", None, FormatFamily.NETCDF4),
        ("netCDF-4", None, FormatFamily.NETCDF4),
        (None, "s3://bucket/file.nc", FormatFamily.NETCDF4),
        (None, "s3://bucket/file.nc4", FormatFamily.NETCDF4),
        ("HDF5", None, FormatFamily.HDF5),
        ("HDF-EOS5", None, FormatFamily.HDF5),
        (None, "s3://bucket/file.h5", FormatFamily.HDF5),
        ("HDF", None, FormatFamily.HDF4),
        ("HDF-EOS2", None, FormatFamily.HDF4),
        ("Zarr", None, FormatFamily.ZARR),
        (None, "s3://bucket/store.zarr", FormatFamily.ZARR),
        ("GeoTIFF", None, FormatFamily.GEOTIFF),
        ("Cloud-Optimized GeoTIFF", None, FormatFamily.GEOTIFF),
        # CMR also distributes the unhyphenated and bare-TIFF variants in the wild.
        ("Cloud Optimized GeoTIFF", None, FormatFamily.GEOTIFF),
        ("TIFF", None, FormatFamily.GEOTIFF),
        ("BigTIFF", None, FormatFamily.GEOTIFF),
        (None, "s3://bucket/scene.tif", FormatFamily.GEOTIFF),
        ("FITS", None, FormatFamily.FITS),
        ("DMR++", None, FormatFamily.DMRPP),
        (None, "s3://bucket/scene.dmrpp", FormatFamily.DMRPP),
        ("PDF", None, None),
        ("Shapefile", None, None),
        ("CSV", "s3://bucket/table.csv", None),
        (None, None, None),
    ],
)
def test_classify_format(declared, url, expected):
    assert classify_format(declared, url) == expected


def test_format_family_values_are_strings():
    # used as DuckDB column values
    assert FormatFamily.NETCDF4.value == "NetCDF4"
    assert FormatFamily.HDF5.value == "HDF5"
    assert FormatFamily.GEOTIFF.value == "GeoTIFF"
