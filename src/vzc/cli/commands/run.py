"""``run`` subcommand: attempt + render + provenance digest (release / preview snapshot)."""

from __future__ import annotations

from typing import cast

import click

from vzc.cli import AccessMode


def register(group: click.Group) -> None:
    @group.command("run")
    @click.option(
        "--snapshot-date",
        "snapshot_date",
        type=str,
        default=None,
        help="ISO date for the snapshot (e.g. 2026-02-15). Defaults to "
        "[tool.uv] exclude-newer in pyproject.toml.",
    )
    @click.option(
        "--label",
        type=str,
        default=None,
        help="Tags the snapshot as a preview with this label. The output "
        "file is named <snapshot_date>-<label>.summary.json.",
    )
    @click.option(
        "--description",
        type=str,
        default=None,
        help="Optional one-line description; only meaningful with --label.",
    )
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="external",
    )
    def run_cmd(
        snapshot_date: str | None,
        label: str | None,
        description: str | None,
        access: str,
    ) -> None:
        """Run attempt + render and emit a ``*.summary.json`` digest.

        With ``--access external`` (default), reads from the cache at
        ``NASA_VZ_SURVEY_CACHE_DIR`` (default ``~/.cache/nasa-virtual-zarr-survey``).
        Run ``prefetch`` first to populate the cache; missing granules will
        fail fast in attempt.

        Reads ``config/locked_sample.json`` and ``[tool.uv] exclude-newer``
        from ``pyproject.toml``. Writes the digest under
        ``docs/results/history/<slug>.summary.json``.
        """
        from vzc.snapshot import SnapshotError, run as _run

        try:
            out = _run(
                snapshot_date=snapshot_date,
                label=label,
                description=description,
                access=cast(AccessMode, access),
            )
        except SnapshotError as e:
            raise click.ClickException(str(e)) from e
        click.echo(f"wrote {out}")
