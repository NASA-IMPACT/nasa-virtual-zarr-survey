"""``sample`` subcommand: pick N granules per collection."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import click

from nasa_virtual_zarr_survey.cli import DEFAULT_DB, AccessMode
from nasa_virtual_zarr_survey.cli._summaries import _sample_summary


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option("--n-bins", type=int, default=5, help="Granules per collection.")
    @click.option("--daac", type=str, default=None, help="Restrict to one DAAC.")
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="direct",
        help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 compute). "
        "'external' uses HTTPS URLs with EDL bearer token.",
    )
    @click.option(
        "--verify-dmrpp/--no-verify-dmrpp",
        "verify_dmrpp",
        default=False,
        help="HEAD each constructed .dmrpp sidecar URL and null it out on 404. "
        "Off by default (relies on the collection's UMM-S association as the signal); "
        "turn on for a one-time audit. Costs one extra request per sampled granule.",
    )
    def sample(
        db_path: Path,
        n_bins: int,
        daac: str | None,
        access: str,
        verify_dmrpp: bool,
    ) -> None:
        """Phase 2 (sample): pick N granules stratified across each collection's CMR revision_date ordering."""
        from nasa_virtual_zarr_survey.sample import run_sample

        run_sample(
            db_path,
            n_bins=n_bins,
            only_daac=daac,
            access=cast(AccessMode, access),
            verify_dmrpp=verify_dmrpp,
        )
        click.echo(_sample_summary(db_path))
