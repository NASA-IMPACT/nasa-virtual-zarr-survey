"""``prefetch`` subcommand: pre-warm the cache in popularity-rank order."""

from __future__ import annotations

import click

from vzc.cli import DEFAULT_CACHE_MAX_SIZE
from vzc.cli._options import _parse_size


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--cache-max-size",
        "cache_max_size",
        type=str,
        default=DEFAULT_CACHE_MAX_SIZE,
        envvar="NASA_VZ_SURVEY_CACHE_MAX_SIZE",
        help="Soft cap on cache size; checked at collection boundaries, so the "
        "collection that crosses the cap finishes writing before the run stops.",
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
    @click.option(
        "--max-granule-size",
        "max_granule_size",
        type=str,
        default=None,
        help="Skip collections that have any sampled granule larger than this "
        "(e.g., '5GB'). Granules with unknown size pass through.",
    )
    def prefetch(
        cache_max_size: str,
        verbose: bool,
        collection: str | None,
        max_granule_size: str | None,
    ) -> None:
        """Phase 2.5 (prefetch): pre-warm the cache in popularity-rank order.

        HTTPS-only — the single writer of the on-disk cache. ``attempt
        --access external`` reads from the cache and fails fast on miss.

        The cache directory comes from ``NASA_VZ_SURVEY_CACHE_DIR`` (env);
        default is ``~/.cache/nasa-virtual-zarr-survey``. Reads
        ``output/state.json``.
        """
        from vzc.pipeline._prefetch import prefetch as _prefetch

        cache_max_bytes = _parse_size(cache_max_size)
        max_granule_bytes = _parse_size(max_granule_size) if max_granule_size else None
        summary = _prefetch(
            collection=collection,
            max_granule_bytes=max_granule_bytes,
            cache_max_bytes=cache_max_bytes,
            verbose=verbose,
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
