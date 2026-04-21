"""CLI entry point."""
from __future__ import annotations

import warnings
from pathlib import Path

import click

from nasa_virtual_zarr_survey import __version__

warnings.filterwarnings(
    "ignore",
    message=r"As of version 1\.0, `DataGranule\.size` will be accessed as an attribute",
    category=FutureWarning,
    module=r"earthaccess\..*",
)
warnings.filterwarnings(
    "ignore",
    message=r"Numcodecs codecs are not in the Zarr version 3 specification",
    category=UserWarning,
)

DEFAULT_DB = Path("output/survey.duckdb")
DEFAULT_RESULTS = Path("output/results")
DEFAULT_REPORT = Path("output/report.md")


def _discover_summary(db_path: Path) -> str:
    from nasa_virtual_zarr_survey.db import connect, init_schema
    con = connect(db_path)
    init_schema(con)
    total = con.execute("SELECT count(*) FROM collections").fetchone()[0]
    skipped = con.execute(
        "SELECT count(*) FROM collections WHERE skip_reason IS NOT NULL"
    ).fetchone()[0]
    array_like = total - skipped
    return (
        f"discover: {total} collections "
        f"({array_like} array-like, {skipped} skipped as non-array format)"
    )


def _skipped_breakdown(db_path: Path, limit: int | None = None) -> str:
    from nasa_virtual_zarr_survey.db import connect, init_schema
    con = connect(db_path)
    init_schema(con)

    by_format = con.execute("""
        SELECT COALESCE(format_declared, '(null)') AS fmt, count(*) AS n
        FROM collections
        WHERE skip_reason = 'non_array_format'
        GROUP BY fmt
        ORDER BY n DESC, fmt
    """).fetchall()

    if not by_format:
        return "Skipped collections: none."

    lines = ["Skipped collections by format:"]
    for fmt, n in by_format:
        lines.append(f"  {n:4d}  {fmt}")

    q = """
        SELECT concept_id, daac, short_name, version, format_declared
        FROM collections
        WHERE skip_reason = 'non_array_format'
        ORDER BY daac, short_name
    """
    if limit is not None:
        q += f" LIMIT {int(limit)}"
    rows = con.execute(q).fetchall()

    lines.append("")
    lines.append(f"Individual skipped collections ({len(rows)}):")
    for concept_id, daac, short_name, version, fmt in rows:
        lines.append(
            f"  {concept_id:30s}  {(daac or '-'):14s}  "
            f"{(fmt or '-'):24s}  {short_name} v{version}"
        )
    return "\n".join(lines)


def _sample_summary(db_path: Path) -> str:
    from nasa_virtual_zarr_survey.db import connect, init_schema
    con = connect(db_path)
    init_schema(con)
    n_gran = con.execute("SELECT count(*) FROM granules").fetchone()[0]
    n_coll = con.execute(
        "SELECT count(DISTINCT collection_concept_id) FROM granules"
    ).fetchone()[0]
    return f"sample: {n_gran} granules across {n_coll} collections"


def _attempt_summary(db_path: Path, results_dir: Path, this_run: int) -> str:
    from nasa_virtual_zarr_survey.db import connect, init_schema
    con = connect(db_path)
    init_schema(con)
    total_granules = con.execute("SELECT count(*) FROM granules").fetchone()[0]

    if total_granules == 0:
        return (
            "attempt: 0 new attempts (the granules table is empty; "
            "run 'sample' or 'discover' first)"
        )

    shards = list(results_dir.glob("**/*.parquet"))
    if not shards:
        if this_run == 0:
            return (
                f"attempt: 0 new attempts "
                f"({total_granules} granules pending, 0 results written; "
                "if you expected attempts to happen, check --daac filters or logs above)"
            )
        return (
            f"attempt: {this_run} new attempts "
            f"(0 of {total_granules} total granules complete; no results written yet)"
        )

    glob = str(results_dir / "**" / "*.parquet")
    q = f"""
        SELECT
            count(*) AS total,
            sum(CASE WHEN parse_success THEN 1 ELSE 0 END) AS parsed,
            sum(CASE WHEN dataset_success THEN 1 ELSE 0 END) AS datasetable,
            sum(CASE WHEN success THEN 1 ELSE 0 END) AS succeeded
        FROM read_parquet('{glob}', union_by_name=true, hive_partitioning=true)
    """
    total, parsed, datasetable, succeeded = con.execute(q).fetchone()
    parsed = parsed or 0
    datasetable = datasetable or 0
    succeeded = succeeded or 0

    if this_run == 0 and total >= total_granules:
        return (
            f"attempt: 0 new attempts "
            f"(all {total_granules} sampled granules already have results)"
        )

    pending = total_granules - total
    if this_run == 0 and pending > 0:
        return (
            f"attempt: 0 new attempts "
            f"({pending} granules still pending, {total} already have results). "
            f"If you expected work to happen, check --daac filter or error above."
        )

    return (
        f"attempt: {this_run} new attempts "
        f"({total} of {total_granules} total granules complete; "
        f"{parsed} parsed, {datasetable} datasetable, {succeeded} fully succeeded)"
    )


@click.group()
def cli() -> None:
    """Survey cloud-hosted NASA CMR collections for VirtualiZarr compatibility."""


@cli.command()
def version() -> None:
    """Print the package version."""
    click.echo(__version__)


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--limit", type=int, default=None,
              help="Cap on total collections (cloud-hosted mode).")
@click.option("--top", "top_total", type=int, default=None,
              help="Fetch the top-N most-used collections TOTAL (ranked by CMR usage_score), "
                   "distributed across EOSDIS providers.")
@click.option("--top-per-provider", "top_per_provider", type=int, default=None,
              help="Fetch the top-N most-used collections PER provider (ranked by CMR usage_score).")
@click.option("--skipped", "show_skipped", is_flag=True, default=False,
              help="After discover completes, print the non-array-format breakdown.")
def discover(
    db_path: Path, limit: int | None,
    top_total: int | None, top_per_provider: int | None,
    show_skipped: bool,
) -> None:
    """Phase 1: enumerate CMR collections and write to DuckDB."""
    from nasa_virtual_zarr_survey.discover import run_discover

    flags = [n for n, v in (("limit", limit), ("top", top_total),
                            ("top-per-provider", top_per_provider)) if v is not None]
    if len(flags) > 1:
        raise click.UsageError(
            f"--{', --'.join(flags)} are mutually exclusive; pass only one"
        )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_discover(
        db_path, limit=limit,
        top_per_provider=top_per_provider, top_total=top_total,
    )
    click.echo(_discover_summary(db_path))
    if show_skipped:
        click.echo("")
        click.echo(_skipped_breakdown(db_path))


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--n-bins", type=int, default=5, help="Granules per collection.")
@click.option("--daac", type=str, default=None, help="Restrict to one DAAC.")
@click.option("--access", type=click.Choice(["direct", "external"]), default="direct",
              help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 compute). "
                   "'external' uses HTTPS URLs with EDL bearer token.")
def sample(db_path: Path, n_bins: int, daac: str | None, access: str) -> None:
    """Phase 2: pick N granules stratified across each collection's temporal extent."""
    from nasa_virtual_zarr_survey.sample import run_sample
    run_sample(db_path, n_bins=n_bins, only_daac=daac, access=access)
    click.echo(_sample_summary(db_path))


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--results", "results_dir", type=click.Path(path_type=Path), default=DEFAULT_RESULTS)
@click.option("--timeout", "timeout_s", type=int, default=60)
@click.option("--shard-size", type=int, default=500)
@click.option("--daac", type=str, default=None, help="Restrict to one DAAC.")
@click.option("--access", type=click.Choice(["direct", "external"]), default="direct",
              help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 compute). "
                   "'external' uses HTTPS URLs with EDL bearer token.")
def attempt(db_path: Path, results_dir: Path, timeout_s: int, shard_size: int,
            daac: str | None, access: str) -> None:
    """Phase 3: open_virtual_dataset each pending granule, write Parquet rows."""
    from nasa_virtual_zarr_survey.attempt import run_attempt
    n = run_attempt(db_path, results_dir, timeout_s=timeout_s, shard_size=shard_size,
                    only_daac=daac, access=access)
    click.echo(_attempt_summary(db_path, results_dir, n))


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--results", "results_dir", type=click.Path(path_type=Path), default=DEFAULT_RESULTS)
@click.option("--out", "out_path", type=click.Path(path_type=Path), default=DEFAULT_REPORT)
def report(db_path: Path, results_dir: Path, out_path: Path) -> None:
    """Phase 4: render report.md from DuckDB state + Parquet results."""
    from nasa_virtual_zarr_survey.report import run_report
    run_report(db_path, results_dir, out_path)
    click.echo(f"Wrote {out_path}")


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--results", "results_dir", type=click.Path(path_type=Path), default=DEFAULT_RESULTS)
@click.option("--out", "out_path", type=click.Path(path_type=Path), default=DEFAULT_REPORT)
@click.option("--sample", "sample_size", type=int, default=50,
              help="Cap on total collections (cloud-hosted mode).")
@click.option("--top", "top_total", type=int, default=None,
              help="Survey top-N TOTAL collections by usage_score (distributed across providers).")
@click.option("--top-per-provider", "top_per_provider", type=int, default=None,
              help="Survey top-N PER provider by usage_score.")
@click.option("--n-bins", type=int, default=5)
@click.option("--timeout", "timeout_s", type=int, default=60)
@click.option("--access", type=click.Choice(["direct", "external"]), default="direct",
              help="CMR granule access mode. 'direct' uses S3 URLs (requires us-west-2 compute). "
                   "'external' uses HTTPS URLs with EDL bearer token.")
def pilot(
    db_path: Path, results_dir: Path, out_path: Path,
    sample_size: int, top_total: int | None, top_per_provider: int | None,
    n_bins: int, timeout_s: int, access: str,
) -> None:
    """Run discover, sample, attempt, report on a small sample for taxonomy review."""
    from nasa_virtual_zarr_survey.discover import run_discover
    from nasa_virtual_zarr_survey.sample import run_sample
    from nasa_virtual_zarr_survey.attempt import run_attempt
    from nasa_virtual_zarr_survey.report import run_report

    if top_total is not None and top_per_provider is not None:
        raise click.UsageError("--top and --top-per-provider are mutually exclusive")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    if top_per_provider is not None:
        run_discover(db_path, top_per_provider=top_per_provider)
    elif top_total is not None:
        run_discover(db_path, top_total=top_total)
    else:
        run_discover(db_path, limit=sample_size)
    click.echo(_discover_summary(db_path))
    run_sample(db_path, n_bins=n_bins, access=access)
    click.echo(_sample_summary(db_path))
    n_att = run_attempt(db_path, results_dir, timeout_s=timeout_s, access=access)
    click.echo(_attempt_summary(db_path, results_dir, n_att))
    run_report(db_path, results_dir, out_path)
    click.echo(f"Pilot complete. Review errors in {results_dir}, refine taxonomy.py, then run full pipeline.")


if __name__ == "__main__":
    cli()
