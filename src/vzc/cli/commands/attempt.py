"""``attempt`` subcommand: phases 3 and 4 (parse, dataset, datatree)."""

from __future__ import annotations

from typing import cast

import click

from vzc.cli import AccessMode, DEFAULT_RESULTS, DEFAULT_STATE_PATH, configure_logging
from vzc.cli._summaries import _attempt_summary


def register(group: click.Group) -> None:
    @group.command()
    @click.option("--timeout", "timeout_s", type=int, default=60)
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="direct",
        help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 "
        "compute) and never touches the cache. 'external' uses HTTPS URLs and "
        "is cache-only — run prefetch first; missing granules fail fast.",
    )
    @click.option(
        "-v",
        "--verbose",
        is_flag=True,
        default=False,
        help="Print per-collection progress and per-granule pass/fail to stderr.",
    )
    def attempt(
        timeout_s: int,
        access: str,
        verbose: bool,
    ) -> None:
        """Phases 3 and 4 (attempt): parsability + datasetability per granule; write Parquet rows.

        Reads ``output/state.json``. Writes Parquet shards under
        ``output/results/``. With ``--access external``, also reads from the
        on-disk cache at ``NASA_VZ_SURVEY_CACHE_DIR``.
        """
        configure_logging(verbose)
        from vzc.pipeline._attempt import attempt as _attempt

        n = _attempt(
            access=cast(AccessMode, access),
            timeout_s=timeout_s,
        )
        click.echo(_attempt_summary(DEFAULT_STATE_PATH, DEFAULT_RESULTS, n))
