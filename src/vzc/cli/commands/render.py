"""``render`` subcommand: phase 5 + render."""

from __future__ import annotations

from pathlib import Path

import click


def register(group: click.Group) -> None:
    @group.command("render")
    @click.option(
        "--from-data",
        "from_data",
        type=click.Path(path_type=Path),
        default=None,
        help="Regenerate the report from a JSON digest; skip state / Parquet queries.",
    )
    @click.option(
        "--history",
        is_flag=True,
        default=False,
        help="Also render the Coverage-over-time page from committed "
        "*.summary.json digests under docs/results/history/.",
    )
    def render_cmd(from_data: Path | None, history: bool) -> None:
        """Phase 5 + render: generate the report from survey state OR a committed JSON digest.

        Reads ``output/state.json`` and ``output/results/`` by default.
        Writes ``docs/results/index.md`` (+ ``figures/``, + ``history.md``
        when ``--history`` is set).
        """
        from vzc._config import DEFAULT_REPORT
        from vzc.render._orchestrate import render

        render(from_data=from_data, history=history)
        click.echo(f"Wrote {DEFAULT_REPORT}")
        if history:
            from vzc._config import DEFAULT_HISTORY_PAGE

            click.echo(f"Wrote {DEFAULT_HISTORY_PAGE}")
