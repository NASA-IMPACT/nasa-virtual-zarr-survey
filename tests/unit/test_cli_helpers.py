from __future__ import annotations

import pytest

from vzc.cli._options import _parse_size


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
    from vzc._config import (
        DEFAULT_CACHE_MAX_BYTES,
        DEFAULT_CACHE_MAX_SIZE,
    )

    assert _parse_size(DEFAULT_CACHE_MAX_SIZE) == DEFAULT_CACHE_MAX_BYTES


def test_prefetch_cache_cap_default_matches_constant() -> None:
    """prefetch (the only writer) defaults its --cache-max-size to the
    canonical constant. attempt/report/snapshot no longer carry the flag
    after cache simplification."""
    from vzc.__main__ import cli
    from vzc._config import DEFAULT_CACHE_MAX_SIZE

    cmd = cli.commands["prefetch"]
    default = next(p.default for p in cmd.params if p.name == "cache_max_size")
    assert default == DEFAULT_CACHE_MAX_SIZE
