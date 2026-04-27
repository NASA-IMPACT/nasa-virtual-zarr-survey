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
