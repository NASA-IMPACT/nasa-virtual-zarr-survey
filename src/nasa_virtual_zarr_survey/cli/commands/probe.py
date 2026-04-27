"""``probe`` subcommand: emit a runnable investigation script."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, cast

import click

from nasa_virtual_zarr_survey.cli import DEFAULT_DB


def register(group: click.Group) -> None:
    @group.command()
    @click.argument("concept_id")
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--out",
        "out_dir",
        type=click.Path(path_type=Path),
        default=None,
        help="Directory to write probe_<id>.py. Defaults to stdout.",
    )
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="direct",
        help="Granule access mode. 'direct' uses S3 URLs (requires us-west-2 compute). "
        "'external' uses HTTPS URLs with EDL bearer token. "
        "Probe may make 0–2 CMR calls at gen time depending on local DB state.",
    )
    def probe(
        concept_id: str,
        db_path: Path,
        out_dir: Path | None,
        access: str,
    ) -> None:
        """Emit a runnable probe script for investigating a CMR collection or granule.

        Use this for collections that were skipped at discover time (no Parquet
        failures to ``repro``) or any concept ID you want to poke regardless of
        survey state.
        """
        from nasa_virtual_zarr_survey.probe import generate_script, resolve_target

        target = resolve_target(
            db_path, concept_id, cast(Literal["direct", "external"], access)
        )
        script = generate_script(target)

        if out_dir is None:
            click.echo(script)
            return

        out_dir.mkdir(parents=True, exist_ok=True)
        suffix_id = (
            target.collection_concept_id
            if target.kind == "collection"
            else target.granule_concept_id
        ) or concept_id
        path = out_dir / f"probe_{suffix_id}.py"
        path.write_text(script)
        click.echo(f"wrote {path}")
