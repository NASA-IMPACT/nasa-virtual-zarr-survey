from __future__ import annotations

import pytest

from nasa_virtual_zarr_survey.cli._options import _parse_size


@pytest.mark.parametrize(
    "value,expected",
    [
        ("100", 100),
        ("1024", 1024),
        ("1KB", 1024),
        ("1MB", 1024**2),
        ("50GB", 50 * 1024**3),
        ("2.5TB", int(2.5 * 1024**4)),
    ],
)
def test_parse_size_known_units(value: str, expected: int) -> None:
    assert _parse_size(value) == expected


def test_parse_size_rejects_garbage() -> None:
    import click

    with pytest.raises(click.BadParameter):
        _parse_size("garbage")


def test_defaults_cache_cap_consistent() -> None:
    """``DEFAULT_CACHE_MAX_BYTES`` must equal ``_parse_size(DEFAULT_CACHE_MAX_SIZE)``.

    Guards against the historical drift where the same cap lived as a
    bytes constant, a click default string, and a separate StoreCache
    fallback — and the three got out of sync.
    """
    from nasa_virtual_zarr_survey._defaults import (
        DEFAULT_CACHE_MAX_BYTES,
        DEFAULT_CACHE_MAX_SIZE,
    )

    assert _parse_size(DEFAULT_CACHE_MAX_SIZE) == DEFAULT_CACHE_MAX_BYTES


def test_attempt_and_prefetch_share_cache_cap_default() -> None:
    """attempt and prefetch must resolve to the same --cache-max-size default."""
    from nasa_virtual_zarr_survey.__main__ import cli
    from nasa_virtual_zarr_survey._defaults import DEFAULT_CACHE_MAX_SIZE

    def _cache_max_size_default(command_name: str) -> str:
        cmd = cli.commands[command_name]
        for param in cmd.params:
            if param.name == "cache_max_size":
                return param.default
        raise AssertionError(f"{command_name} has no --cache-max-size option")

    assert _cache_max_size_default("attempt") == DEFAULT_CACHE_MAX_SIZE
    assert _cache_max_size_default("prefetch") == DEFAULT_CACHE_MAX_SIZE
    assert _cache_max_size_default("report") == DEFAULT_CACHE_MAX_SIZE
    assert _cache_max_size_default("snapshot") == DEFAULT_CACHE_MAX_SIZE
