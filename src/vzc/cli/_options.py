"""Human-friendly size parser shared by CLI subcommands."""

from __future__ import annotations

import re

import click

_SIZE_RE = re.compile(r"^\s*([\d_.]+)\s*([KMGT]B?)?\s*$", re.IGNORECASE)
_SIZE_UNITS = {
    None: 1,
    "K": 1024,
    "KB": 1024,
    "M": 1024**2,
    "MB": 1024**2,
    "G": 1024**3,
    "GB": 1024**3,
    "T": 1024**4,
    "TB": 1024**4,
}


def _parse_size(value: str) -> int:
    """Parse a human-friendly byte count: '50GB', '500MB', '1024'."""
    m = _SIZE_RE.match(value)
    if not m:
        raise click.BadParameter(f"unrecognized size: {value!r}")
    number = float(m.group(1).replace("_", ""))
    unit = m.group(2).upper() if m.group(2) else None
    return int(number * _SIZE_UNITS[unit])
