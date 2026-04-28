"""Unit tests for vzc.pipeline._inspect.

These tests exercise the per-format inspectors against tiny fixtures generated
by ``tests/fixtures/inspect/make_fixtures.py``. If the fixtures are missing,
the file-format-specific tests are skipped with a hint to run the generator.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from vzc.core.formats import FormatFamily
from vzc.pipeline._inspect import (
    INSPECT_JSON_BEGIN,
    INSPECT_JSON_END,
    inspect_url,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "inspect"
GENERATOR_HINT = (
    "Run `uv run python tests/fixtures/inspect/make_fixtures.py` "
    "to generate the inspector fixtures."
)


def _extract_json(out: str) -> dict:
    m = re.search(
        re.escape(INSPECT_JSON_BEGIN) + r"\s*(.*?)\s*" + re.escape(INSPECT_JSON_END),
        out,
        re.S,
    )
    assert m is not None, f"sentinel block not found in:\n{out}"
    return json.loads(m.group(1))


# ---------------------------------------------------------------------------
# Dispatcher behavior
# ---------------------------------------------------------------------------


def test_inspect_unsupported_family_emits_minimal_record(capsys) -> None:
    # HDF4 has no inspector registered.
    inspect_url(url="s3://x/y", family=FormatFamily.HDF4, store=object())
    payload = _extract_json(capsys.readouterr().out)
    assert payload["family"] == "HDF4"
    assert payload["url"] == "s3://x/y"
    assert payload["supported"] is False


def test_inspect_handles_internal_error(capsys) -> None:
    """A bogus URL/store combination produces an error block, not a crash."""
    inspect_url(
        url="s3://nonexistent/bucket/key.h5",
        family=FormatFamily.HDF5,
        store=object(),  # has no .get(); will raise inside the inspector
    )
    payload = _extract_json(capsys.readouterr().out)
    assert payload["supported"] is True
    assert "error_type" in payload


# ---------------------------------------------------------------------------
# Per-format inspectors (require generated fixtures)
# ---------------------------------------------------------------------------


def test_inspect_hdf5_dumps_groups_and_datasets(capsys) -> None:
    fixture = FIXTURES / "two_groups.h5"
    if not fixture.exists():
        pytest.skip(GENERATOR_HINT)

    inspect_url(url=f"file://{fixture}", family=FormatFamily.HDF5, store=None)
    payload = _extract_json(capsys.readouterr().out)
    groups = payload["groups"]
    assert "/science" in groups
    assert "/metadata" in groups
    sci_temp = next(d for d in groups["/science"]["datasets"] if d["name"] == "temp")
    assert sci_temp["dtype"] == "float32"
    assert sci_temp["shape"] == [3, 4]
    assert sci_temp["chunks"] == [3, 4]
    assert sci_temp["compression"] == "gzip"


def test_inspect_zarr(capsys) -> None:
    fixture = FIXTURES / "simple.zarr"
    if not fixture.exists():
        pytest.skip(GENERATOR_HINT)

    inspect_url(url=f"file://{fixture}", family=FormatFamily.ZARR, store=None)
    payload = _extract_json(capsys.readouterr().out)
    arrays = payload["arrays"]
    assert any(a["name"] == "temp" for a in arrays)
    temp = next(a for a in arrays if a["name"] == "temp")
    assert temp["shape"] == [3, 4]


def test_inspect_netcdf3(capsys) -> None:
    fixture = FIXTURES / "classic.nc"
    if not fixture.exists():
        pytest.skip(GENERATOR_HINT)

    inspect_url(url=f"file://{fixture}", family=FormatFamily.NETCDF3, store=None)
    payload = _extract_json(capsys.readouterr().out)
    assert payload["dimensions"] == {"time": 5}
    temp = next(v for v in payload["variables"] if v["name"] == "temp")
    assert temp["dimensions"] == ["time"]


def test_inspect_geotiff(capsys) -> None:
    fixture = FIXTURES / "tiled.tif"
    if not fixture.exists():
        pytest.skip(GENERATOR_HINT)

    inspect_url(url=f"file://{fixture}", family=FormatFamily.GEOTIFF, store=None)
    payload = _extract_json(capsys.readouterr().out)
    ifd0 = payload["ifds"][0]
    assert ifd0["shape"] == [64, 64]
    assert ifd0["tile"] == [32, 32]
    compression = ifd0["compression"].lower()
    assert "deflate" in compression or "zlib" in compression or "adobe" in compression


def test_inspect_dmrpp(capsys) -> None:
    fixture = FIXTURES / "example.dmrpp"
    if not fixture.exists():
        pytest.skip(GENERATOR_HINT)

    inspect_url(url=f"file://{fixture}", family=FormatFamily.DMRPP, store=None)
    payload = _extract_json(capsys.readouterr().out)
    temp = next(v for v in payload["variables"] if v["name"] == "temp")
    assert temp["dtype"] == "Float32"
    assert temp["compression"] == "deflate"


def test_inspect_fits(capsys) -> None:
    fixture = FIXTURES / "two_hdus.fits"
    if not fixture.exists():
        pytest.skip(GENERATOR_HINT)

    inspect_url(url=f"file://{fixture}", family=FormatFamily.FITS, store=None)
    payload = _extract_json(capsys.readouterr().out)
    names = [h["name"] for h in payload["hdus"]]
    assert "IMG" in names
