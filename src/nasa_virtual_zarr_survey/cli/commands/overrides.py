"""``validate-overrides`` and ``lock-sample`` subcommands."""

from __future__ import annotations

from pathlib import Path

import click

from nasa_virtual_zarr_survey.cli import DEFAULT_DB


def register(group: click.Group) -> None:
    @group.command(name="validate-overrides")
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--config",
        "config_path",
        type=click.Path(path_type=Path),
        default=Path("config/collection_overrides.toml"),
    )
    def validate_overrides_cmd(db_path: Path, config_path: Path) -> None:
        """Validate the override TOML against the collections in the survey DB."""
        from nasa_virtual_zarr_survey.db import init_schema, session
        from nasa_virtual_zarr_survey.formats import FormatFamily
        from nasa_virtual_zarr_survey.overrides import OverrideError, OverrideRegistry

        reg = OverrideRegistry.from_toml(config_path)
        format_for: dict[str, FormatFamily] = {}
        with session(db_path) as con:
            init_schema(con)
            for cid, fam_str in con.execute(
                "SELECT concept_id, format_family FROM collections "
                "WHERE format_family IS NOT NULL"
            ).fetchall():
                try:
                    format_for[cid] = FormatFamily(fam_str)
                except ValueError:
                    continue
        try:
            reg.validate(format_for=format_for)
        except OverrideError as e:
            raise click.ClickException(str(e)) from e
        click.echo(
            f"OK: validated {len(reg._by_id)} override entries against "
            f"{len(format_for)} collections"
        )

    @group.command(name="lock-sample")
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--out",
        "out_path",
        type=click.Path(path_type=Path),
        default=Path("config/locked_sample.json"),
        help="Path to write the locked sample JSON.",
    )
    def lock_sample_cmd(db_path: Path, out_path: Path) -> None:
        """Write a deterministic locked sample JSON from the current DB.

        Run after `discover && sample` produces the desired sample. The output
        is committed and consumed by snapshot runs (see scripts/run_snapshot.sh).
        """
        from nasa_virtual_zarr_survey.lock_sample import write_locked_sample

        written = write_locked_sample(db_path, out_path)
        click.echo(f"wrote {written}")
