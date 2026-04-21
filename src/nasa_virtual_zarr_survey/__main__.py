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


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--n-bins", type=int, default=5, help="Granules per collection.")
@click.option("--daac", type=str, default=None, help="Restrict to one DAAC.")
def sample(db_path: Path, n_bins: int, daac: str | None) -> None:
    """Phase 2: pick N granules stratified across each collection's temporal extent."""
    from nasa_virtual_zarr_survey.sample import run_sample
    n = run_sample(db_path, n_bins=n_bins, only_daac=daac)
    click.echo(f"Sampled {n} granules into {db_path}")


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--results", "results_dir", type=click.Path(path_type=Path), default=DEFAULT_RESULTS)
@click.option("--timeout", "timeout_s", type=int, default=60)
@click.option("--shard-size", type=int, default=500)
@click.option("--daac", type=str, default=None, help="Restrict to one DAAC.")
def attempt(db_path: Path, results_dir: Path, timeout_s: int, shard_size: int, daac: str | None) -> None:
    """Phase 3: open_virtual_dataset each pending granule, write Parquet rows."""
    from nasa_virtual_zarr_survey.attempt import run_attempt
    n = run_attempt(db_path, results_dir, timeout_s=timeout_s, shard_size=shard_size, only_daac=daac)
    click.echo(f"Attempted {n} granules; wrote Parquet shards to {results_dir}")


if __name__ == "__main__":
    cli()
