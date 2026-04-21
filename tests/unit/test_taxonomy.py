import pytest

from nasa_virtual_zarr_survey.taxonomy import Bucket, classify


@pytest.mark.parametrize(
    "error_type,error_message,expected",
    [
        ("NoParserAvailable", "No VirtualiZarr parser registered for HDF4", Bucket.NO_PARSER),
        ("TimeoutError", "timed out after 60s", Bucket.TIMEOUT),
        ("PermissionError", "403 Forbidden", Bucket.FORBIDDEN),
        ("ClientError", "Unauthorized", Bucket.FORBIDDEN),
        ("OSError", "Unable to open file (not a valid HDF5 file)", Bucket.CANT_OPEN_FILE),
        ("ValueError", "signature of a valid netCDF4 file", Bucket.CANT_OPEN_FILE),
        ("NotImplementedError", "variable length chunks not supported", Bucket.VARIABLE_CHUNKS),
        ("NotImplementedError", "codec X not supported", Bucket.UNSUPPORTED_CODEC),
        ("NotImplementedError", "filter pipeline element not supported", Bucket.UNSUPPORTED_FILTER),
        ("KeyError", "'compound' dtype", Bucket.COMPOUND_DTYPE),
        ("TypeError", "dtype not supported: string", Bucket.STRING_DTYPE),
        ("Exception", "who knows", Bucket.OTHER),
        (None, None, Bucket.SUCCESS),
    ],
)
def test_classify(error_type, error_message, expected):
    assert classify(error_type, error_message) is expected
