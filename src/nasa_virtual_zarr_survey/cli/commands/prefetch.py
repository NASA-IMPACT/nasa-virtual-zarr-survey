"""``prefetch`` subcommand: pre-warm the cache in popularity-rank order."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import click

from nasa_virtual_zarr_survey.cli import (
    DEFAULT_CACHE_DIR,
    DEFAULT_CACHE_MAX_SIZE,
    DEFAULT_DB,
    AccessMode,
)
from nasa_virtual_zarr_survey.cli._options import (
    _max_granule_size_option,
    _parse_size,
)


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--cache-dir",
        type=click.Path(path_type=Path),
        default=None,
        envvar="NASA_VZ_SURVEY_CACHE_DIR",
        help=f"Cache directory (default: {DEFAULT_CACHE_DIR}).",
    )
    @click.option(
        "--cache-max-size",
        "cache_max_size",
        type=str,
        default=DEFAULT_CACHE_MAX_SIZE,
        envvar="NASA_VZ_SURVEY_CACHE_MAX_SIZE",
        help="Soft cap on cache size; checked at collection boundaries, so the "
        "collection that crosses the cap finishes writing before the run stops. "
        "Shares the default with attempt/report/snapshot — bump it via the flag "
        "or NASA_VZ_SURVEY_CACHE_MAX_SIZE if prefetching needs more headroom.",
    )
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="direct",
        help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 "
        "compute). 'external' uses HTTPS URLs with EDL bearer token. Must match "
        "the URLs already in the granules table.",
    )
    @click.option(
        "-v",
        "--verbose",
        is_flag=True,
        default=False,
        help="Print a per-granule ok/hit/fail line in addition to the per-collection "
        "summary that's always emitted to stderr.",
    )
    @click.option(
        "--collection",
        type=str,
        default=None,
        help="Restrict to one CMR collection concept ID. Bypasses the "
        "popularity_rank requirement — useful for retrying a single collection "
        "that previously failed.",
    )
    @_max_granule_size_option
    def prefetch(
        db_path: Path,
        cache_dir: Path | None,
        cache_max_size: str,
        access: str,
        verbose: bool,
        collection: str | None,
        max_granule_size: str | None,
    ) -> None:
        """Phase 2.5 (prefetch): pre-warm the cache in popularity-rank order."""
        from nasa_virtual_zarr_survey.prefetch import run_prefetch

        effective_cache_dir = cache_dir or DEFAULT_CACHE_DIR
        cache_max_bytes = _parse_size(cache_max_size)
        max_granule_bytes = _parse_size(max_granule_size) if max_granule_size else None
        summary = run_prefetch(
            db_path,
            cache_dir=effective_cache_dir,
            cache_max_bytes=cache_max_bytes,
            access=cast(AccessMode, access),
            verbose=verbose,
            collection=collection,
            max_granule_bytes=max_granule_bytes,
        )
        bytes_gb = summary["bytes_added"] / 1024**3
        stopped = summary["stopped_at_rank"]
        skipped_oversize = summary.get("collections_skipped_oversize", 0)
        skip_str = (
            f", {skipped_oversize} skipped (oversize)" if skipped_oversize else ""
        )
        click.echo(
            f"prefetch: considered {summary['collections_considered']} collection(s)"
            f"{skip_str}, "
            f"fetched {summary['granules_fetched']} granule(s) "
            f"({bytes_gb:.1f} GB added), "
            f"{summary['granules_failed']} failure(s)."
        )
        if stopped:
            click.echo(f"  stopped at popularity rank {stopped} (cap reached).")
        else:
            click.echo("  walked through every ranked collection (cap not reached).")
