"""``repro`` subcommand: emit reproducer scripts for failing granules."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, cast

import click

from nasa_virtual_zarr_survey.cli import DEFAULT_DB, DEFAULT_RESULTS
from nasa_virtual_zarr_survey.cli._probe_hint import _probe_hint


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--results",
        "results_dir",
        type=click.Path(path_type=Path),
        default=DEFAULT_RESULTS,
    )
    @click.option(
        "--bucket",
        type=str,
        default=None,
        help="Filter by taxonomy bucket (e.g., UNSUPPORTED_CODEC).",
    )
    @click.option(
        "--phase",
        type=click.Choice(["parse", "dataset"]),
        default=None,
        help="Filter by which phase failed. Defaults to either.",
    )
    @click.option(
        "--limit",
        type=int,
        default=None,
        help="Max scripts to emit (default: 1 per CONCEPT_ID, 3 per --bucket).",
    )
    @click.option(
        "--out",
        "out_dir",
        type=click.Path(path_type=Path),
        default=None,
        help="Directory to write .py files. Defaults to stdout.",
    )
    @click.option(
        "--overrides",
        "overrides_path",
        type=click.Path(path_type=Path),
        default=Path("config/collection_overrides.toml"),
        help="Path to the per-collection overrides TOML file.",
    )
    @click.option(
        "--no-overrides",
        "no_overrides",
        is_flag=True,
        default=False,
        help="Render the repro without baking in any configured overrides.",
    )
    @click.argument("concept_id", required=False)
    def repro(
        db_path: Path,
        results_dir: Path,
        bucket: str | None,
        phase: str | None,
        limit: int | None,
        out_dir: Path | None,
        overrides_path: Path,
        no_overrides: bool,
        concept_id: str | None,
    ) -> None:
        """Emit a self-contained reproducer Python script for a failing granule."""
        from nasa_virtual_zarr_survey.overrides import OverrideRegistry
        from nasa_virtual_zarr_survey.repro import find_failures, generate_script

        if (concept_id is None) == (bucket is None):
            raise click.UsageError("Provide exactly one of CONCEPT_ID or --bucket.")

        default_limit = 1 if concept_id else 3
        effective_limit = limit if limit is not None else default_limit

        collection_id = (
            concept_id if concept_id and concept_id.startswith("C") else None
        )
        granule_id = concept_id if concept_id and concept_id.startswith("G") else None

        rows = find_failures(
            db_path,
            results_dir,
            collection_concept_id=collection_id,
            granule_concept_id=granule_id,
            bucket=bucket,
            phase=cast(Literal["parse", "dataset"] | None, phase),
            limit=effective_limit,
        )
        if not rows:
            message = "No matching failures found."
            if concept_id is not None:
                hint = _probe_hint(db_path, results_dir, concept_id)
                if hint:
                    message = f"{message}\n{hint}"
            raise click.UsageError(message)

        reg = None if no_overrides else OverrideRegistry.from_toml(overrides_path)

        def _override_for(row):
            return (
                None if reg is None else reg.for_collection(row.collection_concept_id)
            )

        if out_dir is None:
            for i, row in enumerate(rows, 1):
                if len(rows) > 1:
                    click.echo(
                        f"# --- SCRIPT {i}/{len(rows)}: {row.granule_concept_id} ({row.bucket}) ---"
                    )
                click.echo(generate_script(row, override=_override_for(row)))
        else:
            out_dir.mkdir(parents=True, exist_ok=True)
            for row in rows:
                path = out_dir / f"repro_{row.granule_concept_id}.py"
                path.write_text(generate_script(row, override=_override_for(row)))
                click.echo(f"wrote {path}")
