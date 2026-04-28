"""``discover`` subcommand: enumerate CMR collections and write to ``state.json``."""

from __future__ import annotations

from dataclasses import asdict
from typing import Literal, cast

import click

from vzc.cli._listings import (
    _render_collection_listing,
    _skipped_format_breakdown,
)


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--limit",
        type=int,
        default=None,
        help="Cap on total collections (cloud-hosted mode).",
    )
    @click.option(
        "--top",
        type=int,
        default=None,
        help="Fetch the global top-N most-used collections by CMR usage_score.",
    )
    @click.option(
        "--top-per-provider",
        "top_per_provider",
        type=int,
        default=None,
        help="Fetch the top-N most-used collections PER provider.",
    )
    @click.option(
        "--list",
        "list_mode",
        type=click.Choice(["none", "skipped", "array", "all"]),
        default="none",
        help="Listing emitted alongside the aggregate counts.",
    )
    @click.option(
        "--dry-run",
        "dry_run",
        is_flag=True,
        default=False,
        help="Fetch and classify collections without writing to state.json.",
    )
    def discover(
        limit: int | None,
        top: int | None,
        top_per_provider: int | None,
        list_mode: str,
        dry_run: bool,
    ) -> None:
        """Phase 1 (discover): enumerate CMR collections and write to ``output/state.json``."""
        from vzc.cmr._discover import (
            build_collection_rows,
            fetch_collection_dicts,
        )
        from vzc.cmr._discover import discover as _discover

        flags = [
            n
            for n, v in (
                ("limit", limit),
                ("top", top),
                ("top-per-provider", top_per_provider),
            )
            if v is not None
        ]
        if len(flags) > 1:
            raise click.UsageError(
                f"--{', --'.join(flags)} are mutually exclusive; pass only one"
            )

        if dry_run:
            dicts, score_map = fetch_collection_dicts(
                limit=limit, top_per_provider=top_per_provider, top_total=top
            )
            rows = build_collection_rows(dicts, score_map=score_map)
            total = len(rows)
            skipped = sum(1 for r in rows if r.skip_reason)
            array_like = total - skipped
            click.echo(
                f"discover (dry-run): {total} collections "
                f"({array_like} array-like, {skipped} skipped as non-array format)"
            )
        else:
            n = _discover(limit=limit, top=top, top_per_provider=top_per_provider)
            # Re-load to render the listing if requested.
            from vzc.state._io import load_state
            from vzc._config import DEFAULT_STATE_PATH

            state = load_state(DEFAULT_STATE_PATH)
            skipped = sum(1 for c in state.collections if c.skip_reason)
            array_like = len(state.collections) - skipped
            click.echo(
                f"discover: {n} collections "
                f"({array_like} array-like, {skipped} skipped as non-array format)"
            )
            if list_mode == "none":
                return
            list_choice = cast(Literal["skipped", "array", "all"], list_mode)
            row_dicts = [asdict(r) for r in state.collections]
            score_map = None
            if any(r.popularity_rank is not None for r in state.collections):
                score_map = {
                    r.concept_id: (r.popularity_rank, r.usage_score)
                    for r in state.collections
                    if r.popularity_rank is not None
                }
            if list_choice == "skipped":
                click.echo("")
                click.echo(_skipped_format_breakdown(row_dicts))
            click.echo("")
            click.echo(
                _render_collection_listing(
                    row_dicts, list_mode=list_choice, score_map=score_map
                )
            )
            return

        if list_mode == "none":
            return
        list_choice = cast(Literal["skipped", "array", "all"], list_mode)
        row_dicts = [asdict(r) for r in rows]
        if list_choice == "skipped":
            click.echo("")
            click.echo(_skipped_format_breakdown(row_dicts))
        click.echo("")
        click.echo(
            _render_collection_listing(
                row_dicts, list_mode=list_choice, score_map=score_map
            )
        )
