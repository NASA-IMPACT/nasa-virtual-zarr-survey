"""``investigate`` subcommand: emit a runnable Python script for one concept ID.

Replaces the older ``probe`` (native exploration) and ``repro`` (survey-path
reproduction) commands with a single tool: ``investigate ID --mode {virtual|native}``.
Pipe the output to ``uv run python -`` to execute now, or write to a file
for later iteration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, cast

import click

from vzc.cli import AccessMode


def register(group: click.Group) -> None:
    @group.command()
    @click.argument("concept_id")
    @click.option(
        "--mode",
        type=click.Choice(["virtual", "native"]),
        default="virtual",
        help="``virtual`` reproduces the survey's VirtualiZarr code path. "
        "``native`` emits an exploration script using the format-appropriate "
        "library (h5py, netCDF4, astropy, zarr, tifffile).",
    )
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="external",
        help="Granule access mode the script binds to.",
    )
    @click.option(
        "--out",
        "out_path",
        type=click.Path(path_type=Path),
        default=None,
        help="Write the script to this file. Defaults to stdout.",
    )
    def investigate(
        concept_id: str,
        mode: str,
        access: str,
        out_path: Path | None,
    ) -> None:
        """Emit a runnable Python script for investigating a CMR collection or granule.

        Reads from the default state at ``output/state.json`` if it exists;
        otherwise falls back to one or two CMR calls to resolve the URL.
        """
        from vzc.pipeline._investigate import investigate as _investigate

        script = _investigate(
            concept_id,
            mode=cast(Literal["virtual", "native"], mode),
            access=cast(AccessMode, access),
        )

        if out_path is None:
            click.echo(script)
            return
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(script)
        click.echo(f"wrote {out_path}", err=True)
