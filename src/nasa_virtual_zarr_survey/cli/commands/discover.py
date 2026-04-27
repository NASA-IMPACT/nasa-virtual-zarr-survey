"""``discover`` subcommand: enumerate CMR collections and write to DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, cast

import click

from nasa_virtual_zarr_survey.cli import DEFAULT_DB
from nasa_virtual_zarr_survey.cli._listings import (
    _render_collection_listing,
    _skipped_format_breakdown,
)


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--limit",
        type=int,
        default=None,
        help="Cap on total collections (cloud-hosted mode).",
    )
    @click.option(
        "--top",
        "top_total",
        type=int,
        default=None,
        help="Fetch the global top-N most-used collections by CMR usage_score "
        "(a single popular provider can dominate).",
    )
    @click.option(
        "--top-per-provider",
        "top_per_provider",
        type=int,
        default=None,
        help="Fetch the top-N most-used collections PER provider (ranked by CMR usage_score).",
    )
    @click.option(
        "--list",
        "list_mode",
        type=click.Choice(["none", "skipped", "array", "all"]),
        default="none",
        help="Listing emitted alongside the aggregate counts. "
        "'skipped' prints the (format_declared, skip_reason) breakdown plus a "
        "table of skipped collections. 'array' lists array-like collections only "
        "(those that would feed `sample`). 'all' lists every collection with a "
        "skip_reason column. In --top/--top-per-provider modes the listing is "
        "sorted by popularity rank.",
    )
    @click.option(
        "--dry-run",
        "dry_run",
        is_flag=True,
        default=False,
        help="Fetch and classify collections without writing to the DB.",
    )
    def discover(
        db_path: Path,
        limit: int | None,
        top_total: int | None,
        top_per_provider: int | None,
        list_mode: str,
        dry_run: bool,
    ) -> None:
        """Phase 1 (discover): enumerate CMR collections and write to DuckDB."""
        from datetime import datetime, timezone

        from nasa_virtual_zarr_survey.db import init_schema, session
        from nasa_virtual_zarr_survey.discover import (
            collection_row_from_umm,
            fetch_collection_dicts,
            persist_collections,
            sampling_mode_string,
        )

        flags = [
            n
            for n, v in (
                ("limit", limit),
                ("top", top_total),
                ("top-per-provider", top_per_provider),
            )
            if v is not None
        ]
        if len(flags) > 1:
            raise click.UsageError(
                f"--{', --'.join(flags)} are mutually exclusive; pass only one"
            )

        dicts, score_map = fetch_collection_dicts(
            limit=limit,
            top_per_provider=top_per_provider,
            top_total=top_total,
        )
        rows = [collection_row_from_umm(d) for d in dicts]
        total = len(rows)
        skipped = sum(1 for r in rows if r["skip_reason"])
        array_like = total - skipped

        if dry_run:
            click.echo(
                f"discover (dry-run): {total} collections "
                f"({array_like} array-like, {skipped} skipped as non-array format)"
            )
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            with session(db_path) as con:
                init_schema(con)
                persist_collections(con, dicts, score_map=score_map)
                con.execute(
                    "INSERT OR REPLACE INTO run_meta (key, value, updated_at) "
                    "VALUES (?, ?, ?)",
                    [
                        "sampling_mode",
                        sampling_mode_string(limit, top_per_provider, top_total),
                        datetime.now(timezone.utc),
                    ],
                )
            click.echo(
                f"discover: {total} collections "
                f"({array_like} array-like, {skipped} skipped as non-array format)"
            )

        if list_mode == "none":
            return
        list_choice = cast(Literal["skipped", "array", "all"], list_mode)
        if list_choice == "skipped":
            click.echo("")
            click.echo(_skipped_format_breakdown(rows))
        click.echo("")
        click.echo(
            _render_collection_listing(rows, list_mode=list_choice, score_map=score_map)
        )
