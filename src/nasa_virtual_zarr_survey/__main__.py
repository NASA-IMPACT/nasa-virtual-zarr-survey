"""CLI entry point."""
import click


@click.group()
def cli() -> None:
    """Survey cloud-hosted NASA CMR collections for VirtualiZarr compatibility."""


@cli.command()
def version() -> None:
    """Print the package version."""
    from nasa_virtual_zarr_survey import __version__
    click.echo(__version__)


if __name__ == "__main__":
    cli()
