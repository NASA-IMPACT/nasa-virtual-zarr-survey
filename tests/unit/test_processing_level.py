from __future__ import annotations

import pytest

from nasa_virtual_zarr_survey.processing_level import parse_rank


@pytest.mark.parametrize(
    "value,expected",
    [
        ("0", 0),
        ("1", 1),
        ("1A", 1),
        ("1B", 1),
        ("1C", 1),
        ("L1B", 1),
        ("2", 2),
        ("2A", 2),
        ("L2", 2),
        ("3", 3),
        ("L3", 3),
        ("4", 4),
        ("L4", 4),
        (" 3 ", 3),
    ],
)
def test_parse_rank_recognizes_known_levels(value: str, expected: int) -> None:
    assert parse_rank(value) == expected


@pytest.mark.parametrize(
    "value",
    ["", None, "Not Provided", "Not Applicable", "NA", "unknown", "L", "L9", "9"],
)
def test_parse_rank_returns_none_for_unknown(value: str | None) -> None:
    assert parse_rank(value) is None
