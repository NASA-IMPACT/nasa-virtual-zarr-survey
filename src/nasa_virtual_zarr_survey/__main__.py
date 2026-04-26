"""CLI entry point."""

from __future__ import annotations

import re
import shutil
import warnings
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence, cast

import click

from nasa_virtual_zarr_survey import __version__

AccessMode = Literal["direct", "external"]

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
warnings.filterwarnings(
    "ignore",
    message=r"Imagecodecs codecs are not in the Zarr version 3 specification",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"In a future version, xarray will not decode the variable .* into a timedelta64 dtype",
    category=FutureWarning,
)

DEFAULT_DB = Path("output/survey.duckdb")
DEFAULT_RESULTS = Path("output/results")
DEFAULT_REPORT = Path("docs/results/index.md")
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "nasa-virtual-zarr-survey"
DEFAULT_CACHE_MAX_BYTES = 50 * 1024**3


_SIZE_RE = re.compile(r"^\s*([\d_.]+)\s*([KMGT]B?)?\s*$", re.IGNORECASE)
_SIZE_UNITS = {
    None: 1,
    "K": 1024,
    "KB": 1024,
    "M": 1024**2,
    "MB": 1024**2,
    "G": 1024**3,
    "GB": 1024**3,
    "T": 1024**4,
    "TB": 1024**4,
}


def _parse_size(value: str) -> int:
    """Parse a human-friendly byte count: '50GB', '500MB', '1024'."""
    m = _SIZE_RE.match(value)
    if not m:
        raise click.BadParameter(f"unrecognized size: {value!r}")
    number = float(m.group(1).replace("_", ""))
    unit = m.group(2).upper() if m.group(2) else None
    return int(number * _SIZE_UNITS[unit])


def _cache_options(f=None, *, default_use_cache: bool = False):
    """Apply --cache, --cache-dir, --cache-max-size to a Click command.

    Decorators are applied bottom-up, so to keep the original `--help` order
    (cache, cache-dir, cache-max-size) the option closest to the function
    must be applied last.

    Usage: ``@_cache_options`` to default ``--cache`` off (most subcommands),
    or ``@_cache_options(default_use_cache=True)`` for ``snapshot``, where
    cache reuse across runs is the whole point.
    """

    def _apply(fn):
        fn = click.option(
            "--cache-max-size",
            "cache_max_size",
            type=str,
            default="50GB",
            help="Soft cap on total cache size; supports human-readable units "
            "(e.g. 50GB, 500MB).",
        )(fn)
        fn = click.option(
            "--cache-dir",
            type=click.Path(path_type=Path),
            default=None,
            envvar="NASA_VZ_SURVEY_CACHE_DIR",
            help=f"Cache directory (default: {DEFAULT_CACHE_DIR}).",
        )(fn)
        fn = click.option(
            "--cache/--no-cache",
            "use_cache",
            default=default_use_cache,
            help="Cache fetched granule bytes to disk so repeat runs hit local "
            "disk instead of re-fetching.",
        )(fn)
        return fn

    if f is None:
        return _apply
    return _apply(f)


def _resolve_cache_params(
    use_cache: bool, cache_dir: Path | None, cache_max_size: str
) -> tuple[Path | None, int]:
    """Return ``(effective_cache_dir, cache_max_bytes)`` for ``run_attempt``."""
    effective_cache_dir = (cache_dir or DEFAULT_CACHE_DIR) if use_cache else None
    return effective_cache_dir, _parse_size(cache_max_size)


def _discover_summary(db_path: Path) -> str:
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(db_path)
    init_schema(con)
    total = (con.execute("SELECT count(*) FROM collections").fetchone() or (0,))[0]
    skipped = (
        con.execute(
            "SELECT count(*) FROM collections WHERE skip_reason IS NOT NULL"
        ).fetchone()
        or (0,)
    )[0]
    array_like = total - skipped
    return (
        f"discover: {total} collections "
        f"({array_like} array-like, {skipped} skipped as non-array format)"
    )


def _skipped_format_breakdown(rows: Sequence[Mapping[str, Any]]) -> str:
    """Aggregate ``(format_declared, skip_reason)`` counts for skipped rows."""
    from collections import Counter

    counts: Counter = Counter(
        ((r.get("format_declared") or "(null)"), r["skip_reason"])
        for r in rows
        if r.get("skip_reason")
    )
    if not counts:
        return "Skipped collections: none."
    lines = ["Skipped collections by format:"]
    for (fmt, reason), n in counts.most_common():
        lines.append(f"  {n:4d}  {fmt}  ({reason})")
    return "\n".join(lines)


def _render_collection_listing(
    rows: Sequence[Mapping[str, Any]],
    *,
    list_mode: Literal["skipped", "array", "all"],
    score_map: dict[str, tuple[int, int | None]] | None,
) -> str:
    """Render a fixed-width table of collections per ``--list <mode>``.

    ``score_map`` is ``{concept_id: (rank, usage_score)}`` in top-N modes; when
    ``None``, the rank and usage_score columns render blank. Sorting follows
    rank in top-N modes and ``(daac, short_name)`` otherwise. Within a top-N
    listing, ``usage_score`` may itself be ``None`` for collections without a
    community-usage-metrics entry; those rows render with a blank score column.
    """
    if list_mode == "skipped":
        filtered = [r for r in rows if r.get("skip_reason")]
    elif list_mode == "array":
        filtered = [r for r in rows if not r.get("skip_reason")]
    else:
        filtered = list(rows)

    if score_map is not None:

        def _sort_key(r: Mapping[str, Any]):
            rs = score_map.get(r.get("concept_id") or "")
            # Rows without a score (e.g., dropped by the search backend) sort last.
            return (rs[0] if rs else 1_000_000_000, r.get("concept_id") or "")

        filtered.sort(key=_sort_key)
    else:
        filtered.sort(
            key=lambda r: ((r.get("daac") or ""), (r.get("short_name") or ""))
        )

    headers = [
        "rank",
        "usage_score",
        "concept_id",
        "daac",
        "fmt_family",
        "fmt_declared",
        "opendap",
        "proc_lvl",
        "short_name v version",
        "skip_reason",
        "url",
    ]
    table_rows: list[list[str]] = []
    for r in filtered:
        cid = r.get("concept_id") or ""
        rs = score_map.get(cid) if score_map else None
        rank = str(rs[0]) if rs else ""
        score = str(rs[1]) if rs and rs[1] is not None else ""
        sn = r.get("short_name") or ""
        ver = r.get("version") or ""
        sn_ver = f"{sn} v{ver}" if sn or ver else ""
        url = f"https://search.earthdata.nasa.gov/search?q={cid}" if cid else ""
        table_rows.append(
            [
                rank,
                score,
                cid,
                r.get("daac") or "",
                r.get("format_family") or "—",
                r.get("format_declared") or "(null)",
                "Y" if r.get("has_cloud_opendap") else "",
                r.get("processing_level") or "",
                sn_ver,
                r.get("skip_reason") or "",
                url,
            ]
        )

    widths = [len(h) for h in headers]
    for row in table_rows:
        for i, cell in enumerate(row):
            if len(cell) > widths[i]:
                widths[i] = len(cell)

    NUMERIC_COLS = {0, 1}  # rank, usage_score render right-aligned

    def _fmt(cells: list[str]) -> str:
        parts = [
            cell.rjust(w) if i in NUMERIC_COLS else cell.ljust(w)
            for i, (cell, w) in enumerate(zip(cells, widths))
        ]
        return "  ".join(parts).rstrip()

    lines = [_fmt(headers)]
    for row in table_rows:
        lines.append(_fmt(row))
    return "\n".join(lines)


def _sample_summary(db_path: Path) -> str:
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(db_path)
    init_schema(con)
    n_gran = (con.execute("SELECT count(*) FROM granules").fetchone() or (0,))[0]
    n_coll = (
        con.execute(
            "SELECT count(DISTINCT collection_concept_id) FROM granules"
        ).fetchone()
        or (0,)
    )[0]
    return f"sample: {n_gran} granules across {n_coll} collections"


def _probe_hint(db_path: Path, results_dir: Path, concept_id: str) -> str | None:
    """Return a 'try probe' hint when the concept ID is skipped or un-attempted.

    Triggers when:
    - the collection has ``skip_reason IS NOT NULL`` (no granules attempted), OR
    - the concept ID has zero rows in granules AND zero rows in the Parquet log.

    Otherwise returns None (the original "No matching failures found" message
    stands unchanged).
    """
    if not db_path.exists():
        return None

    from nasa_virtual_zarr_survey.db import connect, init_schema

    is_collection = concept_id.startswith("C")
    is_granule = concept_id.startswith("G")
    if not (is_collection or is_granule):
        return None

    con = connect(db_path)
    try:
        init_schema(con)
        skip_reason: str | None = None
        if is_collection:
            row = con.execute(
                "SELECT skip_reason FROM collections WHERE concept_id = ?",
                [concept_id],
            ).fetchone()
            if row is not None and row[0] is not None:
                skip_reason = row[0]

        # Rows in granules table?
        if is_collection:
            n_gran = (
                con.execute(
                    "SELECT count(*) FROM granules WHERE collection_concept_id = ?",
                    [concept_id],
                ).fetchone()
                or (0,)
            )[0]
        else:
            n_gran = (
                con.execute(
                    "SELECT count(*) FROM granules WHERE granule_concept_id = ?",
                    [concept_id],
                ).fetchone()
                or (0,)
            )[0]
    finally:
        con.close()

    # Rows in Parquet log?
    n_parquet = 0
    shards = list(results_dir.glob("**/*.parquet"))
    if shards:
        import duckdb

        con2 = duckdb.connect(":memory:")
        col = "collection_concept_id" if is_collection else "granule_concept_id"
        glob = str(results_dir / "**" / "*.parquet")
        try:
            n_parquet = (
                con2.execute(
                    f"SELECT count(*) FROM read_parquet({glob!r}, "
                    f"union_by_name=true, hive_partitioning=true) WHERE {col} = ?",
                    [concept_id],
                ).fetchone()
                or (0,)
            )[0]
        except Exception:
            n_parquet = 0
        finally:
            con2.close()

    if skip_reason is not None:
        return (
            f"Hint: this collection has skip_reason={skip_reason!r} "
            "(no granules attempted).\n"
            f"Try `nasa-virtual-zarr-survey probe {concept_id}` to investigate."
        )
    if n_gran == 0 and n_parquet == 0:
        kind = "collection" if is_collection else "granule"
        return (
            f"Hint: this {kind} has no granules sampled and no Parquet rows.\n"
            f"Try `nasa-virtual-zarr-survey probe {concept_id}` to investigate."
        )
    return None


def _attempt_summary(db_path: Path, results_dir: Path, this_run: int) -> str:
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(db_path)
    init_schema(con)
    total_granules = (con.execute("SELECT count(*) FROM granules").fetchone() or (0,))[
        0
    ]

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
    total, parsed, datasetable, succeeded = con.execute(q).fetchone() or (0, 0, 0, 0)
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

    from nasa_virtual_zarr_survey.db import connect, init_schema
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
        con = connect(db_path)
        init_schema(con)
        persist_collections(con, dicts)
        con.execute(
            "INSERT OR REPLACE INTO run_meta (key, value, updated_at) VALUES (?, ?, ?)",
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


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option("--n-bins", type=int, default=5, help="Granules per collection.")
@click.option("--daac", type=str, default=None, help="Restrict to one DAAC.")
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
    "Off by default (relies on the collection's UMM-S association as the signal); "
    "turn on for a one-time audit. Costs one extra request per sampled granule.",
)
def sample(
    db_path: Path,
    n_bins: int,
    daac: str | None,
    access: str,
    verify_dmrpp: bool,
) -> None:
    """Phase 2 (sample): pick N granules stratified across each collection's temporal extent."""
    from nasa_virtual_zarr_survey.sample import run_sample

    run_sample(
        db_path,
        n_bins=n_bins,
        only_daac=daac,
        access=cast(AccessMode, access),
        verify_dmrpp=verify_dmrpp,
    )
    click.echo(_sample_summary(db_path))


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
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
    "--results", "results_dir", type=click.Path(path_type=Path), default=DEFAULT_RESULTS
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
    )
    if locked_sample_path is None:
        click.echo(_attempt_summary(db_path, results_dir, n))
    else:
        click.echo(f"attempt: {n} new attempts (sourced from {locked_sample_path})")


@cli.command(name="validate-overrides")
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path),
    default=Path("config/collection_overrides.toml"),
)
def validate_overrides_cmd(db_path: Path, config_path: Path) -> None:
    """Validate the override TOML against the collections in the survey DB."""
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from nasa_virtual_zarr_survey.formats import FormatFamily
    from nasa_virtual_zarr_survey.overrides import OverrideError, OverrideRegistry

    reg = OverrideRegistry.from_toml(config_path)
    con = connect(db_path)
    init_schema(con)
    format_for: dict[str, FormatFamily] = {}
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


@cli.command(name="lock-sample")
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
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


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option(
    "--locked-sample",
    "locked_sample_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Source the session from a locked sample JSON instead of --db. "
    "Mutually exclusive with --from-data.",
)
@click.option(
    "--access",
    type=click.Choice(["direct", "external"]),
    default="direct",
    help="Access mode used when constructing a session from --locked-sample.",
)
@click.option(
    "--results", "results_dir", type=click.Path(path_type=Path), default=DEFAULT_RESULTS
)
@click.option(
    "--out", "out_path", type=click.Path(path_type=Path), default=DEFAULT_REPORT
)
@click.option(
    "--export",
    "export_to",
    type=click.Path(path_type=Path),
    default=None,
    help="Also write a compact JSON digest suitable for regenerating the report in CI.",
)
@click.option(
    "--from-data",
    "from_data",
    type=click.Path(path_type=Path),
    default=None,
    help="Regenerate the report from a JSON digest; skip DuckDB/Parquet queries.",
)
@click.option(
    "--snapshot-date",
    "snapshot_date",
    type=str,
    default=None,
    help="ISO date for the snapshot (e.g., 2026-02-15). Drives "
    "snapshot_date / snapshot_kind in the exported summary.",
)
@click.option(
    "--uv-lock",
    "uv_lock_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to a uv.lock file to hash and reference as uv_lock_sha256.",
)
@click.option(
    "--preview-manifest",
    "preview_manifest_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to a config/snapshot_previews/*.toml. When set, snapshot_kind "
    "becomes 'preview' and label/description/git_overrides come from the "
    "manifest. Mutually exclusive with --uv-lock.",
)
@click.option(
    "--no-render",
    "no_render",
    is_flag=True,
    default=False,
    help="Skip the markdown + figures output. Useful when only the "
    "--export JSON digest is wanted.",
)
def report(
    db_path: Path,
    locked_sample_path: Path | None,
    access: str,
    results_dir: Path,
    out_path: Path,
    export_to: Path | None,
    from_data: Path | None,
    snapshot_date: str | None,
    uv_lock_path: Path | None,
    preview_manifest_path: Path | None,
    no_render: bool,
) -> None:
    """Phase 5 + render: generate the report from survey state OR a committed JSON digest."""
    from nasa_virtual_zarr_survey.db_session import SurveySession
    from nasa_virtual_zarr_survey.report import run_report

    if export_to is not None and from_data is not None:
        raise click.UsageError("--export and --from-data are mutually exclusive")
    if locked_sample_path is not None and from_data is not None:
        raise click.UsageError("--locked-sample and --from-data are mutually exclusive")
    if uv_lock_path is not None and preview_manifest_path is not None:
        raise click.UsageError(
            "--uv-lock and --preview-manifest are mutually exclusive"
        )

    session: SurveySession | None
    if from_data is not None:
        session = None
    elif locked_sample_path is not None:
        session = SurveySession.from_locked_sample(
            locked_sample_path, access=cast(AccessMode, access)
        )
    else:
        session = SurveySession.from_duckdb(db_path)

    run_report(
        session,
        results_dir=results_dir,
        out_path=out_path,
        export_to=export_to,
        from_data=from_data,
        snapshot_date=snapshot_date,
        locked_sample_path=locked_sample_path,
        uv_lock_path=uv_lock_path,
        preview_manifest_path=preview_manifest_path,
        no_render=no_render,
    )
    if export_to:
        click.echo(f"Wrote digest to {export_to}")
    if not no_render:
        click.echo(f"Wrote {out_path}")


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
@click.option(
    "--results", "results_dir", type=click.Path(path_type=Path), default=DEFAULT_RESULTS
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
        raise click.UsageError("--top and --top-per-provider are mutually exclusive")

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
    run_sample(db_path, n_bins=n_bins, access=access_mode, verify_dmrpp=verify_dmrpp)
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


@cli.command()
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
    help="Required when [tool.uv.sources] has git overrides; names the "
    "preview snapshot's output file.",
)
@click.option(
    "--description",
    type=str,
    default=None,
    help="Optional one-line description for previews.",
)
@click.option(
    "--preview-manifest",
    "preview_manifest_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Use a pre-curated config/snapshot_previews/*.toml manifest instead "
    "of reading pyproject.toml.",
)
@click.option(
    "--locked-sample",
    "locked_sample_path",
    type=click.Path(path_type=Path),
    default=Path("config/locked_sample.json"),
)
@click.option(
    "--access",
    type=click.Choice(["direct", "external"]),
    default="external",
)
@click.option(
    "--uv-lock",
    "uv_lock_path",
    type=click.Path(path_type=Path),
    default=Path("uv.lock"),
    help="Path to the active uv.lock; copied beside the digest for releases.",
)
@click.option(
    "--results",
    "results_dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Per-snapshot results directory. Defaults to output/snapshots/<slug>/results.",
)
@click.option(
    "--history-dir",
    type=click.Path(path_type=Path),
    default=Path("docs/results/history"),
)
@_cache_options(default_use_cache=True)
def snapshot(
    snapshot_date: str | None,
    label: str | None,
    description: str | None,
    preview_manifest_path: Path | None,
    locked_sample_path: Path,
    access: str,
    uv_lock_path: Path,
    results_dir: Path | None,
    history_dir: Path,
    use_cache: bool,
    cache_dir: Path | None,
    cache_max_size: str,
) -> None:
    """Run attempt + report and emit a `*.summary.json` digest.

    Reads ``[tool.uv] exclude-newer`` for the snapshot date and
    ``[tool.uv.sources]`` for git overrides — the same pyproject.toml that
    pinned the env. Pass ``--preview-manifest`` to bypass pyproject.toml
    detection in favor of a pre-curated manifest file.

    Caching is on by default: subsequent snapshots against the same locked
    sample reuse fetched granule bytes. Pass ``--no-cache`` to disable.
    """
    from nasa_virtual_zarr_survey.snapshot import SnapshotError, run_snapshot

    effective_cache_dir, cache_max_bytes = _resolve_cache_params(
        use_cache, cache_dir, cache_max_size
    )
    try:
        out = run_snapshot(
            snapshot_date=snapshot_date,
            label=label,
            description=description,
            preview_manifest_path=preview_manifest_path,
            locked_sample_path=locked_sample_path,
            access=cast(AccessMode, access),
            uv_lock_path=uv_lock_path,
            results_dir=results_dir,
            history_dir=history_dir,
            cache_dir=effective_cache_dir,
            cache_max_bytes=cache_max_bytes,
        )
    except SnapshotError as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"wrote {out}")


@cli.command()
@click.option(
    "--history-dir",
    "history_dir",
    type=click.Path(path_type=Path),
    default=Path("docs/results/history"),
    help="Directory holding committed *.summary.json digests.",
)
@click.option(
    "--out",
    "out_path",
    type=click.Path(path_type=Path),
    default=Path("docs/results/history.md"),
    help="Path to write the rendered Coverage-over-time markdown.",
)
@click.option(
    "--intros",
    "intros_path",
    type=click.Path(path_type=Path),
    default=Path("config/feature_introductions.toml"),
    help="Path to feature_introductions.toml.",
)
def history(history_dir: Path, out_path: Path, intros_path: Path) -> None:
    """Render the Coverage-over-time page from committed summary digests."""
    from nasa_virtual_zarr_survey.history import run_history

    warning = run_history(history_dir, out_path, intros_path=intros_path)
    if warning is not None:
        click.echo(warning, err=True)
    click.echo(f"wrote {out_path}")


@cli.command()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
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
    from typing import Literal

    from nasa_virtual_zarr_survey.overrides import OverrideRegistry
    from nasa_virtual_zarr_survey.repro import find_failures, generate_script

    if (concept_id is None) == (bucket is None):
        raise click.UsageError("Provide exactly one of CONCEPT_ID or --bucket.")

    default_limit = 1 if concept_id else 3
    effective_limit = limit if limit is not None else default_limit

    # Disambiguate collection vs granule concept IDs by prefix (C vs G).
    collection_id = concept_id if concept_id and concept_id.startswith("C") else None
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
        return None if reg is None else reg.for_collection(row.collection_concept_id)

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


@cli.command()
@click.argument("concept_id")
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_DB)
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


if __name__ == "__main__":
    cli()
