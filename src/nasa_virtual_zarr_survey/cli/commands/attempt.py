"""``attempt`` subcommand: phases 3 and 4 (parse, dataset, datatree)."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import click

from nasa_virtual_zarr_survey.cli import DEFAULT_DB, DEFAULT_RESULTS, AccessMode
from nasa_virtual_zarr_survey.cli._options import (
    _cache_only_option,
    _cache_options,
    _max_granule_size_option,
    _parse_size,
    _resolve_cache_params,
    require_cache_dir_for_cache_only,
)
from nasa_virtual_zarr_survey.cli._summaries import _attempt_summary


def register(group: click.Group) -> None:
    @group.command()
    @click.option(
        "--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB
    )
    @click.option(
        "--locked-sample",
        "locked_sample_path",
        type=click.Path(path_type=Path),
        default=None,
        help="Path to a config/locked_sample.json. When set, sources collections "
        "and granules from the JSON via an in-memory DuckDB session instead of "
        "reading --db.",
    )
    @click.option(
        "--results",
        "results_dir",
        type=click.Path(path_type=Path),
        default=DEFAULT_RESULTS,
    )
    @click.option("--timeout", "timeout_s", type=int, default=60)
    @click.option("--shard-size", type=int, default=500)
    @click.option("--daac", type=str, default=None, help="Restrict to one DAAC.")
    @click.option(
        "--collection",
        "only_collection",
        type=str,
        default=None,
        help="Restrict to one CMR collection concept ID.",
    )
    @click.option(
        "--access",
        type=click.Choice(["direct", "external"]),
        default="direct",
        help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 compute). "
        "'external' uses HTTPS URLs with EDL bearer token.",
    )
    @_cache_options
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
        help="Run as if config/collection_overrides.toml were empty (vanilla baseline).",
    )
    @click.option(
        "--skip-override-validation",
        "skip_override_validation",
        is_flag=True,
        default=False,
        help="Load the override TOML but skip the startup signature check; "
        "runtime exceptions from incompatible kwargs are captured per attempt.",
    )
    @_max_granule_size_option
    @_cache_only_option
    def attempt(
        db_path: Path,
        locked_sample_path: Path | None,
        results_dir: Path,
        timeout_s: int,
        shard_size: int,
        daac: str | None,
        only_collection: str | None,
        access: str,
        use_cache: bool,
        cache_dir: Path | None,
        cache_max_size: str,
        overrides_path: Path,
        no_overrides: bool,
        skip_override_validation: bool,
        max_granule_size: str | None,
        cache_only: bool,
    ) -> None:
        """Phases 3 and 4 (attempt): parsability + datasetability per granule; write Parquet rows."""
        from nasa_virtual_zarr_survey.attempt import run_attempt
        from nasa_virtual_zarr_survey.db_session import SurveySession

        if locked_sample_path is not None:
            session = SurveySession.from_locked_sample(
                locked_sample_path, access=cast(AccessMode, access)
            )
        else:
            session = SurveySession.from_duckdb(db_path)

        effective_cache_dir, cache_max_bytes = _resolve_cache_params(
            use_cache, cache_dir, cache_max_size
        )
        max_granule_bytes = _parse_size(max_granule_size) if max_granule_size else None
        require_cache_dir_for_cache_only(cache_only, effective_cache_dir)
        n = run_attempt(
            session,
            results_dir,
            timeout_s=timeout_s,
            shard_size=shard_size,
            only_daac=daac,
            only_collection=only_collection,
            access=cast(AccessMode, access),
            cache_dir=effective_cache_dir,
            cache_max_bytes=cache_max_bytes,
            overrides_path=overrides_path,
            no_overrides=no_overrides,
            skip_override_validation=skip_override_validation,
            max_granule_bytes=max_granule_bytes,
            cache_only=cache_only,
        )
        if locked_sample_path is None:
            click.echo(_attempt_summary(db_path, results_dir, n))
        else:
            click.echo(f"attempt: {n} new attempts (sourced from {locked_sample_path})")
