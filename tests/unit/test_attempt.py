from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from nasa_virtual_zarr_survey.attempt import (
    AttemptResult,
    attempt_one,
    dispatch_parser,
)
from nasa_virtual_zarr_survey.formats import FormatFamily


def test_dispatch_parser_maps_known_families():
    p = dispatch_parser(FormatFamily.NETCDF4)
    assert p is not None
    # The HDFParser class comes from virtualizarr.parsers.hdf
    assert type(p).__name__ == "HDFParser"

    assert dispatch_parser(FormatFamily.HDF5) is not None
    assert dispatch_parser(FormatFamily.NETCDF3) is not None
    assert dispatch_parser(FormatFamily.DMRPP) is not None
    assert dispatch_parser(FormatFamily.FITS) is not None
    assert dispatch_parser(FormatFamily.ZARR) is not None


def test_dispatch_parser_returns_none_for_unsupported():
    assert dispatch_parser(FormatFamily.HDF4) is None
    assert dispatch_parser(FormatFamily.GEOTIFF) is None


def test_attempt_one_records_no_parser():
    result = attempt_one(
        url="s3://bucket/file.hdf",
        family=FormatFamily.HDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is False
    assert result.error_type == "NoParserAvailable"
    assert result.parser is None
    assert result.timed_out is False


def test_attempt_one_success(monkeypatch):
    def fake_ovd(url, registry, parser, **kwargs):
        assert url == "s3://bucket/file.nc"
        return MagicMock(name="Dataset")

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.open_virtual_dataset", fake_ovd
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is True
    assert result.error_type is None
    assert result.parser == "HDFParser"
    assert result.duration_s >= 0


def test_attempt_one_captures_exception(monkeypatch):
    def fake_ovd(*_, **__):
        raise ValueError("boom")

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.open_virtual_dataset", fake_ovd
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is False
    assert result.error_type == "ValueError"
    assert "boom" in result.error_message
    assert result.error_traceback is not None


def test_attempt_one_timeout(monkeypatch):
    import time

    def fake_ovd(*_, **__):
        time.sleep(10)

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.open_virtual_dataset", fake_ovd
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=1,
    )
    assert result.success is False
    assert result.timed_out is True
    assert result.error_type == "TimeoutError"
