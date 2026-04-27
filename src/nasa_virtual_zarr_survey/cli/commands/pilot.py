"""``pilot`` subcommand: full discover→sample→attempt→report on a small sample."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import cast

import click

from nasa_virtual_zarr_survey.cli import (
    DEFAULT_DB,
    DEFAULT_REPORT,
    DEFAULT_RESULTS,
    AccessMode,
)
from nasa_virtual_zarr_survey.cli._options import (
    _cache_options,
    _resolve_cache_params,
)
from nasa_virtual_zarr_survey.cli._summaries import (
    _attempt_summary,
    _discover_summary,
    _sample_summary,
)


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
        "--out", "out_path", type=click.Path(path_type=Path), default=DEFAULT_REPORT
    )
    @click.option(
        "--sample",
        "sample_size",
        type=int,
        default=50,
        help="Cap on total collections (cloud-hosted mode).",
    )
    @click.option(
        "--top",
        "top_total",
        type=int,
        default=None,
        help="Survey global top-N collections by usage_score (a single popular provider can dominate).",
    )
    @click.option(
        "--top-per-provider",
        "top_per_provider",
        type=int,
        default=None,
        help="Survey top-N PER provider by usage_score.",
    )
    @click.option("--n-bins", type=int, default=5)
    @click.option("--timeout", "timeout_s", type=int, default=60)
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="direct",
        help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 compute). "
        "'external' uses HTTPS URLs with EDL bearer token.",
    )
    @click.option(
        "--verify-dmrpp/--no-verify-dmrpp",
        "verify_dmrpp",
        default=False,
        help="HEAD each constructed .dmrpp sidecar URL and null it out on 404. "
        "See `sample --help` for the tradeoff.",
    )
    @_cache_options
    @click.option(
        "--clean",
        is_flag=True,
        default=False,
        help="Delete the DuckDB and results directory before running, for a true "
        "end-to-end run that does not reuse prior shards.",
    )
    def pilot(
        db_path: Path,
        results_dir: Path,
        out_path: Path,
        sample_size: int,
        top_total: int | None,
        top_per_provider: int | None,
        n_bins: int,
        timeout_s: int,
        access: str,
        verify_dmrpp: bool,
        use_cache: bool,
        cache_dir: Path | None,
        cache_max_size: str,
        clean: bool,
    ) -> None:
        """Run discover, sample, attempt, report on a small sample for taxonomy review."""
        from nasa_virtual_zarr_survey.attempt import run_attempt
        from nasa_virtual_zarr_survey.db_session import SurveySession
        from nasa_virtual_zarr_survey.discover import run_discover
        from nasa_virtual_zarr_survey.report import run_report
        from nasa_virtual_zarr_survey.sample import run_sample

        if top_total is not None and top_per_provider is not None:
            raise click.UsageError(
                "--top and --top-per-provider are mutually exclusive"
            )

        if clean:
            targets = [p for p in (db_path, results_dir) if p.exists()]
            if targets:
                click.echo("--clean will delete:")
                for p in targets:
                    click.echo(f"  {p}")
                click.confirm("Proceed?", abort=True, default=False)
                if db_path.exists():
                    db_path.unlink()
                if results_dir.exists():
                    shutil.rmtree(results_dir)

        db_path.parent.mkdir(parents=True, exist_ok=True)
        results_dir.mkdir(parents=True, exist_ok=True)

        if top_per_provider is not None:
            run_discover(db_path, top_per_provider=top_per_provider)
        elif top_total is not None:
            run_discover(db_path, top_total=top_total)
        else:
            run_discover(db_path, limit=sample_size)
        click.echo(_discover_summary(db_path))
        access_mode = cast(AccessMode, access)
        run_sample(
            db_path, n_bins=n_bins, access=access_mode, verify_dmrpp=verify_dmrpp
        )
        click.echo(_sample_summary(db_path))
        effective_cache_dir, cache_max_bytes = _resolve_cache_params(
            use_cache, cache_dir, cache_max_size
        )
        session = SurveySession.from_duckdb(db_path)
        n_att = run_attempt(
            session,
            results_dir,
            timeout_s=timeout_s,
            access=access_mode,
            cache_dir=effective_cache_dir,
            cache_max_bytes=cache_max_bytes,
        )
        click.echo(_attempt_summary(db_path, results_dir, n_att))
        summary_path = out_path.parent / "summary.json"
        run_report(session, results_dir, out_path, export_to=summary_path)
        click.echo(f"Wrote {out_path} and {summary_path}")
        click.echo(
            f"Pilot complete. Review errors in {results_dir}, refine taxonomy.py, then run full pipeline."
        )
