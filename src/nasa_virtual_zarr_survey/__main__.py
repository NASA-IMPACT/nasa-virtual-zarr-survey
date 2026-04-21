"""CLI entry point."""
from __future__ import annotations

from pathlib import Path

import click

from nasa_virtual_zarr_survey import __version__

DEFAULT_DB = Path("output/survey.duckdb")
DEFAULT_RESULTS = Path("output/results")
DEFAULT_REPORT = Path("output/report.md")


@click.group()
def cli() -> None:
    """Survey cloud-hosted NASA CMR collections for VirtualiZarr compatibility."""


@cli.command()
def version() -> None:
    """Print the package version."""
    click.echo(__version__)


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--limit", type=int, default=None, help="Max collections to discover.")
def discover(db_path: Path, limit: int | None) -> None:
    """Phase 1: enumerate cloud-hosted EOSDIS collections."""
    from nasa_virtual_zarr_survey.discover import run_discover

    db_path.parent.mkdir(parents=True, exist_ok=True)
    n = run_discover(db_path, limit=limit)
    click.echo(f"Discovered {n} collections into {db_path}")


if __name__ == "__main__":
    cli()
