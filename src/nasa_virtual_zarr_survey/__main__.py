"""CLI entry point.

Builds the click group and registers each subcommand from
``nasa_virtual_zarr_survey.cli.commands``. Each command lives in its own
module with a ``register(group)`` entry point so a single subcommand can be
edited without loading the world.
"""

from __future__ import annotations

import warnings

import click

from nasa_virtual_zarr_survey import __version__
from nasa_virtual_zarr_survey.cli.commands import (
    attempt as _attempt,
    discover as _discover,
    history as _history,
    overrides as _overrides,
    pilot as _pilot,
    prefetch as _prefetch,
    probe as _probe,
    report as _report,
    repro as _repro,
    sample as _sample,
    snapshot as _snapshot,
)

# Suppress noise from underlying libraries during CLI runs. These are run-time
# warnings about API surfaces the survey doesn't drive (e.g. earthaccess'
# `DataGranule.size` migration) — they bury our actual log lines.
warnings.filterwarnings(
    "ignore",
    message=r"As of version 1\.0, `DataGranule\.size` will be accessed as an attribute",
    category=FutureWarning,
    module=r"earthaccess\..*",
)
warnings.filterwarnings(
    "ignore",
    message=r"Numcodecs codecs are not in the Zarr version 3 specification",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"Imagecodecs codecs are not in the Zarr version 3 specification",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"In a future version, xarray will not decode the variable .* into a timedelta64 dtype",
    category=FutureWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"The data type .* does not have a Zarr V3 specification",
    category=FutureWarning,
)


@click.group()
def cli() -> None:
    """Survey cloud-hosted NASA CMR collections for VirtualiZarr compatibility."""


@cli.command()
def version() -> None:
    """Print the package version."""
    click.echo(__version__)


for _mod in (
    _discover,
    _sample,
    _prefetch,
    _attempt,
    _overrides,
    _report,
    _pilot,
    _snapshot,
    _history,
    _repro,
    _probe,
):
    _mod.register(cli)


if __name__ == "__main__":
    cli()
