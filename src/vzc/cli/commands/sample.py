"""``sample`` subcommand: pick N granules per collection."""

from __future__ import annotations

import click

from vzc.cli._summaries import _sample_summary


def register(group: click.Group) -> None:
    @group.command()
    @click.option("--n-bins", type=int, default=5, help="Granules per collection.")
    def sample(n_bins: int) -> None:
        """Phase 2 (sample): pick N granules stratified across each collection's CMR revision_date ordering.

        Records both ``s3://`` (used by ``--access direct``) and ``https://``
        (used by ``--access external``) URLs for every granule, so ``attempt``
        and ``prefetch`` can flip access modes without re-sampling.
        """
        from vzc._config import DEFAULT_STATE_PATH
        from vzc.cmr._sample import sample as _sample

        _sample(n_bins=n_bins)
        click.echo(_sample_summary(DEFAULT_STATE_PATH))
