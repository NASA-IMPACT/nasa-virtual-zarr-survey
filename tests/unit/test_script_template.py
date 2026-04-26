"""Unit tests for nasa_virtual_zarr_survey.script_template."""

from __future__ import annotations

import py_compile
import tempfile
from pathlib import Path

import pytest

from nasa_virtual_zarr_survey.formats import FormatFamily
from nasa_virtual_zarr_survey.script_template import (
    render_cache_argparse,
    render_cache_wiring,
    render_inspect_block,
    render_login_and_store,
)


def _wrap_main(snippet: str, *, extra_imports: str = "") -> str:
    """Wrap a body snippet in enough scaffolding to feed py_compile.

    The snippet is expected to live inside ``def main()`` (4-space indent),
    so we just prepend ``from __future__ ...`` and a ``def main():`` header.
    The wrapper isn't actually run; only ``py_compile`` reads it.
    """
    return (
        "from __future__ import annotations\n"
        f"{extra_imports}"
        "\n"
        "def main() -> None:\n"
        f"{snippet}"
        "    pass\n"
    )


def _assert_compiles(source: str) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(source)
        path = Path(f.name)
    try:
        py_compile.compile(str(path), doraise=True)
    finally:
        path.unlink()


def test_render_login_and_store_s3() -> None:
    snippet = render_login_and_store(
        url="s3://my-bucket/path/file.nc",
        provider="POCLOUD",
        registry_key="s3://my-bucket",
    )
    assert "from obstore.store import S3Store" in snippet
    assert "earthaccess.get_s3_credentials(provider=" in snippet
    assert "'POCLOUD'" in snippet
    assert "S3Store(" in snippet
    assert "bucket='my-bucket'" in snippet
    _assert_compiles(_wrap_main(snippet, extra_imports="import earthaccess\n"))


def test_render_login_and_store_https() -> None:
    snippet = render_login_and_store(
        url="https://archive.example/path/file.nc",
        provider=None,
        registry_key="https://archive.example",
    )
    assert "from obstore.store import HTTPStore" in snippet
    assert "HTTPStore.from_url" in snippet
    assert "'https://archive.example'" in snippet
    assert 'token_dict = getattr(earthaccess.__auth__, "token", None)' in snippet
    _assert_compiles(_wrap_main(snippet, extra_imports="import earthaccess\n"))


def test_render_login_and_store_rejects_unknown_scheme() -> None:
    with pytest.raises(ValueError):
        render_login_and_store(
            url="ftp://nope/file.nc", provider=None, registry_key="ftp://nope"
        )


def test_render_cache_argparse_and_wiring_compile_together() -> None:
    """argparse + cache wiring should produce a syntactically valid block."""
    body = render_cache_argparse() + render_cache_wiring(
        registry_key="https://archive.example"
    )
    assert "--cache" in body
    assert "--cache-dir" in body
    assert "--cache-max-size" in body
    assert "DiskCachingReadableStore" in body
    assert "args.cache" in body
    # Need an in-scope ``store`` so the body type-checks at compile time —
    # py_compile only needs syntactic validity, but in case future tests add
    # more checks, give it a placeholder.
    src = _wrap_main("    store = None\n" + body)
    _assert_compiles(src)


def test_render_inspect_block_bakes_literals() -> None:
    snippet = render_inspect_block(
        url="https://archive.example/file.nc", family=FormatFamily.NETCDF4
    )
    assert "url = 'https://archive.example/file.nc'" in snippet
    assert 'family = FormatFamily("NetCDF4")' in snippet
    assert "inspect_url(url=url, family=family, store=store)" in snippet
