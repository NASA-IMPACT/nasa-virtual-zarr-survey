"""Shared click decorators and the human-friendly size parser.

These were inline in ``__main__.py``; pulled out so subcommand modules can
import only what they need without dragging the rest of the CLI along.
"""

from __future__ import annotations

import re
from pathlib import Path

import click

from nasa_virtual_zarr_survey.cli import DEFAULT_CACHE_DIR

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


def _cache_options(f=None, *, default_use_cache: bool = False):
    """Apply --cache, --cache-dir, --cache-max-size to a Click command.

    Decorators are applied bottom-up, so to keep the original `--help` order
    (cache, cache-dir, cache-max-size) the option closest to the function
    must be applied last.

    Usage: ``@_cache_options`` to default ``--cache`` off (most subcommands),
    or ``@_cache_options(default_use_cache=True)`` for ``snapshot``, where
    cache reuse across runs is the whole point.
    """

    def _apply(fn):
        fn = click.option(
            "--cache-max-size",
            "cache_max_size",
            type=str,
            default="50GB",
            help="Soft cap on total cache size; supports human-readable units "
            "(e.g. 50GB, 500MB).",
        )(fn)
        fn = click.option(
            "--cache-dir",
            type=click.Path(path_type=Path),
            default=None,
            envvar="NASA_VZ_SURVEY_CACHE_DIR",
            help=f"Cache directory (default: {DEFAULT_CACHE_DIR}).",
        )(fn)
        fn = click.option(
            "--cache/--no-cache",
            "use_cache",
            default=default_use_cache,
            help="Cache fetched granule bytes to disk so repeat runs hit local "
            "disk instead of re-fetching.",
        )(fn)
        return fn

    if f is None:
        return _apply
    return _apply(f)


def _resolve_cache_params(
    use_cache: bool, cache_dir: Path | None, cache_max_size: str
) -> tuple[Path | None, int]:
    """Return ``(effective_cache_dir, cache_max_bytes)`` for ``run_attempt``."""
    effective_cache_dir = (cache_dir or DEFAULT_CACHE_DIR) if use_cache else None
    return effective_cache_dir, _parse_size(cache_max_size)


def _max_granule_size_option(f):
    """``--max-granule-size`` flag shared across prefetch/attempt/snapshot."""
    return click.option(
        "--max-granule-size",
        "max_granule_size",
        type=str,
        default=None,
        help="Skip collections that have any sampled granule larger than this "
        "(e.g., '5GB'). Granules with unknown size pass through. "
        "Applied identically across prefetch, attempt, and snapshot.",
    )(f)


def _cache_only_option(f):
    """``--cache-only`` flag for attempt and snapshot.

    Restricts the run to granules already present in --cache-dir. Useful after
    a prefetch run to avoid any network fetches during attempt/snapshot.
    """
    return click.option(
        "--cache-only",
        "cache_only",
        is_flag=True,
        default=False,
        help="Only attempt granules already present in --cache-dir. Skips any "
        "granule that would otherwise be fetched from origin. Pair with "
        "--cache (the default for snapshot) so the cache directory is set.",
    )(f)


def require_cache_dir_for_cache_only(
    cache_only: bool, effective_cache_dir: Path | None
) -> None:
    """Raise click.UsageError when ``--cache-only`` is set but no cache dir resolved.

    Centralizes the guard the attempt/report/snapshot subcommands all need.
    """
    if cache_only and effective_cache_dir is None:
        raise click.UsageError(
            "--cache-only requires --cache (the cache directory must be known)."
        )
