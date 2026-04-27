"""``history`` subcommand: render Coverage-over-time from committed digests."""

from __future__ import annotations

from pathlib import Path

import click


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--history-dir",
        "history_dir",
        type=click.Path(path_type=Path),
        default=Path("docs/results/history"),
        help="Directory holding committed *.summary.json digests.",
    )
    @click.option(
        "--out",
        "out_path",
        type=click.Path(path_type=Path),
        default=Path("docs/results/history.md"),
        help="Path to write the rendered Coverage-over-time markdown.",
    )
    @click.option(
        "--intros",
        "intros_path",
        type=click.Path(path_type=Path),
        default=Path("config/feature_introductions.toml"),
        help="Path to feature_introductions.toml.",
    )
    def history(history_dir: Path, out_path: Path, intros_path: Path) -> None:
        """Render the Coverage-over-time page from committed summary digests."""
        from nasa_virtual_zarr_survey.history import run_history

        warning = run_history(history_dir, out_path, intros_path=intros_path)
        if warning is not None:
            click.echo(warning, err=True)
        click.echo(f"wrote {out_path}")
