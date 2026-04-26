"""Generate self-contained reproducer scripts for failing granules.

Given a collection or granule concept ID (or a failure-bucket filter), queries
the survey results, picks matching failures, and emits a minimal Python script
that reproduces the error outside the survey harness.
"""

from __future__ import annotations

import textwrap
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from nasa_virtual_zarr_survey.overrides import CollectionOverride
from nasa_virtual_zarr_survey.script_template import (
    _registry_key,
    render_cache_argparse,
    render_cache_wiring,
    render_login_and_store,
)


@dataclass
class FailureRow:
    collection_concept_id: str
    granule_concept_id: str
    daac: str | None
    provider: str | None
    format_family: str | None
    parser: str | None
    data_url: str
    https_url: str | None
    phase: Literal["parse", "dataset"]
    error_type: str
    error_message: str
    error_traceback: str | None
    bucket: str  # from taxonomy.classify


# ---------------------------------------------------------------------------
# Parser dispatch table
# ---------------------------------------------------------------------------

_PARSER_TABLE: dict[str, tuple[str, str]] = {
    "HDFParser": (
        "from virtualizarr.parsers.hdf import HDFParser",
        "HDFParser()",
    ),
    "NetCDF3Parser": (
        "from virtualizarr.parsers.netcdf3 import NetCDF3Parser",
        "NetCDF3Parser()",
    ),
    "FITSParser": (
        "from virtualizarr.parsers.fits import FITSParser",
        "FITSParser()",
    ),
    "DMRPPParser": (
        "from virtualizarr.parsers.dmrpp import DMRPPParser",
        "DMRPPParser()",
    ),
    "ZarrParser": (
        "from virtualizarr.parsers.zarr import ZarrParser",
        "ZarrParser()",
    ),
    "VirtualTIFF": (
        "from virtual_tiff import VirtualTIFF",
        "VirtualTIFF()",
    ),
}


def _render_kwargs(kw: Mapping[str, Any]) -> str:
    """Render a kwargs dict as the inside of a function call: 'a=1, b="x"'."""
    if not kw:
        return ""
    return ", ".join(f"{k}={v!r}" for k, v in kw.items())


def _format_url_lines(row: FailureRow) -> str:
    """Return the URL block (one or two lines) shown in a repro docstring.

    Always includes the survey's ``data_url``. When an HTTPS download URL was
    captured at sample time and differs from ``data_url`` (e.g. the survey ran
    under ``--access direct``), include it as a second line so a reader can
    fetch the granule with curl/wget.
    """
    lines = [f"URL: {row.data_url}"]
    if row.https_url and row.https_url != row.data_url:
        lines.append(
            f"Download URL (HTTPS, EDL bearer token required): {row.https_url}"
        )
    return "\n".join(lines)


_DUAL_USE_BLURB = (
    "This script is also a working starting point for non-debugging "
    "virtualization workflows: edit the parser/dataset kwargs (or strip "
    "the failure-context docstring) and treat it as a runnable seed. "
    "For structural inspection, run "
    "``nasa-virtual-zarr-survey probe <granule-or-collection-id>``."
)


def generate_script(  # noqa: C901  (acceptable complexity)
    row: FailureRow,
    override: CollectionOverride | None = None,
) -> str:
    """Render a self-contained Python script that reproduces *row*'s failure."""
    url = row.data_url
    is_s3 = url.startswith("s3://")
    url_lines = _format_url_lines(row)

    # ------------------------------------------------------------------
    # No-parser case: just document the failure.
    # ------------------------------------------------------------------
    if row.parser is None or row.error_type == "NoParserAvailable":
        script_name = f"repro_{row.granule_concept_id}.py"
        tb_section = (
            f"\nTraceback from survey run:\n{textwrap.indent(row.error_traceback, '    ')}\n"
            if row.error_traceback
            else ""
        )
        return (
            f'"""Reproducer for {row.collection_concept_id} / {row.granule_concept_id}.\n'
            f"\n"
            f"DAAC: {row.daac}\n"
            f"Format family: {row.format_family}\n"
            f"Phase that failed: {'Parsability (Phase 3)' if row.phase == 'parse' else 'Datasetability (Phase 4)'}\n"
            f"Bucket: {row.bucket}\n"
            f"\n"
            f"{url_lines}\n"
            f"\n"
            f"Original error observed in survey run:\n"
            f"    {row.error_type}: {row.error_message}\n"
            f"{tb_section}"
            f'"""\n'
            f"# This format family ({row.format_family!r}) has no registered parser in\n"
            f"# the survey as of the run date. There is no automatic reproducer that can\n"
            f"# be generated for a 'NoParserAvailable' failure.\n"
            f"#\n"
            f"# Run with:\n"
            f"#     uv run python {script_name}\n"
            f"raise NotImplementedError(\n"
            f'    "No parser is registered for format family {row.format_family!r}. "\n'
            f'    "Cannot reproduce this failure automatically."\n'
            f")\n"
        )

    # ------------------------------------------------------------------
    # Known-parser case: look up import + construction.
    # ------------------------------------------------------------------
    if row.parser not in _PARSER_TABLE:
        # Unknown parser: emit a TODO stub.
        script_name = f"repro_{row.granule_concept_id}.py"
        return (
            f'"""Reproducer for {row.collection_concept_id} / {row.granule_concept_id}.\n'
            f"\n"
            f"DAAC: {row.daac}\n"
            f"Format family: {row.format_family}\n"
            f"Bucket: {row.bucket}\n"
            f"\n"
            f"{url_lines}\n"
            f"\n"
            f"Original error observed in survey run:\n"
            f"    {row.error_type}: {row.error_message}\n"
            f'"""\n'
            f"# TODO: unknown parser {row.parser!r} - fill in the correct import and\n"
            f"# construction below.\n"
            f"raise NotImplementedError(\n"
            f'    "Unknown parser {row.parser!r}. Cannot generate a reproducer automatically."\n'
            f")\n"
        )

    parser_import, parser_construction = _PARSER_TABLE[row.parser]
    registry_key = _registry_key(url)
    script_name = f"repro_{row.granule_concept_id}.py"

    ov = override or CollectionOverride()
    parser_kwargs_str = _render_kwargs(ov.parser_kwargs)
    if parser_kwargs_str:
        parser_construction = parser_construction.replace(
            "()", f"({parser_kwargs_str})"
        )
    dataset_kwargs_str = _render_kwargs(ov.dataset_kwargs)

    # ------------------------------------------------------------------
    # Docstring
    # ------------------------------------------------------------------
    phase_label = (
        "Parsability (Phase 3)" if row.phase == "parse" else "Datasetability (Phase 4)"
    )
    tb_section = ""
    if row.error_traceback:
        tb_section = (
            "\nTraceback from survey run:\n"
            + textwrap.indent(row.error_traceback.rstrip(), "    ")
            + "\n"
        )

    docstring = (
        f'"""Reproducer for {row.collection_concept_id} / {row.granule_concept_id}.\n'
        f"\n"
        f"DAAC: {row.daac}\n"
        f"Format family: {row.format_family}\n"
        f"Phase that failed: {phase_label}\n"
        f"Bucket: {row.bucket}\n"
        f"\n"
        f"{url_lines}\n"
        f"\n"
        f"Original error observed in survey run:\n"
        f"    {row.error_type}: {row.error_message}\n"
        f"{tb_section}"
        f"\n"
        f"{_DUAL_USE_BLURB}\n"
        f"\n"
        f"Run with:\n"
        f"    uv run python {script_name}\n"
        f'"""\n'
    )

    # ------------------------------------------------------------------
    # Store construction + cache wiring (shared with probe via script_template)
    # ------------------------------------------------------------------
    store_block = render_login_and_store(
        url=url, provider=row.provider, registry_key=registry_key
    )
    cache_block = render_cache_wiring(registry_key=registry_key)
    argparse_block = render_cache_argparse()
    store_import = (
        "from obstore.store import S3Store"
        if is_s3
        else "from obstore.store import HTTPStore"
    )

    # ------------------------------------------------------------------
    # Main body (attempt phase)
    # ------------------------------------------------------------------
    if row.phase == "parse":
        call_lines = (
            "    manifest_store = parser(url=url, registry=registry)\n"
            '    print("Parse succeeded. Manifest store:", manifest_store)\n'
        )
    else:
        call_lines = (
            "    manifest_store = parser(url=url, registry=registry)\n"
            f"    ds = manifest_store.to_virtual_dataset({dataset_kwargs_str})\n"
            '    print("Dataset construction succeeded:")\n'
            "    print(ds)\n"
        )

    body = (
        f"def main() -> None:\n"
        f"{argparse_block}"
        f"\n"
        f"{store_block}"
        f"\n"
        f"{cache_block}"
        f"\n"
        f"    url = {url!r}\n"
        f"    registry = ObjectStoreRegistry({{{registry_key!r}: store}})\n"
        f"\n"
        f"    parser = {parser_construction}\n"
        f"{call_lines}"
        f"\n"
        f'if __name__ == "__main__":\n'
        f"    main()\n"
    )

    script = (
        docstring
        + "from __future__ import annotations\n"
        + "\n"
        + "import earthaccess\n"
        + f"{store_import}\n"
        + "from obspec_utils.registry import ObjectStoreRegistry\n"
        + f"{parser_import}\n"
        + "\n"
        + "\n"
        + body
    )
    return script


def find_failures(
    db_path: Path | str,
    results_dir: Path | str,
    *,
    collection_concept_id: str | None = None,
    granule_concept_id: str | None = None,
    bucket: str | None = None,
    phase: Literal["parse", "dataset"] | None = None,
    limit: int = 1,
) -> list[FailureRow]:
    """Query results.parquet for failures matching the filters.

    Joins the Parquet results with the DuckDB collections/granules tables to
    recover ``data_url`` and ``provider``.  Applies the taxonomy classifier to
    each row and filters by *bucket* and/or *phase* as requested.

    Returns up to *limit* ``FailureRow`` objects.
    """
    import duckdb

    from nasa_virtual_zarr_survey.taxonomy import classify

    results_dir = Path(results_dir)
    db_path = Path(db_path)

    glob = str(results_dir / "**" / "*.parquet")

    # Short-circuit if there are no Parquet shards yet.
    if not list(results_dir.glob("**/*.parquet")):
        return []

    # We need a fresh in-memory connection that can also attach the on-disk DB.
    con = duckdb.connect(":memory:")

    # Attach the survey DB so we can join with collections/granules.
    # If the DB file does not exist yet, use NULLs for provider/data_url.
    has_db = db_path.exists()
    if has_db:
        con.execute(f"ATTACH '{db_path}' AS survey (READ_ONLY)")

    # Concept ID filters.
    conditions: list[str] = []
    params: list[str] = []
    if collection_concept_id is not None:
        conditions.append("r.collection_concept_id = ?")
        params.append(collection_concept_id)
    if granule_concept_id is not None:
        conditions.append("r.granule_concept_id = ?")
        params.append(granule_concept_id)

    where_clause = (
        "WHERE (r.parse_error_type IS NOT NULL OR r.dataset_error_type IS NOT NULL)"
    )
    if conditions:
        where_clause += " AND " + " AND ".join(conditions)

    if has_db:
        join_fragment = """
        LEFT JOIN survey.collections c
            ON c.concept_id = r.collection_concept_id
        LEFT JOIN survey.granules g
            ON g.collection_concept_id = r.collection_concept_id
           AND g.granule_concept_id    = r.granule_concept_id
        """
        provider_col = "c.provider"
        url_col = "g.data_url"
        https_col = "g.https_url"
    else:
        join_fragment = ""
        provider_col = "NULL"
        url_col = "NULL"
        https_col = "NULL"

    query = f"""
        SELECT
            r.collection_concept_id,
            r.granule_concept_id,
            r.daac,
            {provider_col} AS provider,
            r.format_family,
            r.parser,
            {url_col} AS data_url,
            {https_col} AS https_url,
            r.parse_success,
            r.parse_error_type,
            r.parse_error_message,
            r.parse_error_traceback,
            r.dataset_success,
            r.dataset_error_type,
            r.dataset_error_message,
            r.dataset_error_traceback
        FROM read_parquet({glob!r}, union_by_name=true, hive_partitioning=true) r
        {join_fragment}
        {where_clause}
        ORDER BY r.collection_concept_id, r.granule_concept_id
    """

    try:
        rows = con.execute(query, params).fetchall()
    except Exception:
        return []

    results: list[FailureRow] = []
    for row in rows:
        (
            coll_id,
            gran_id,
            daac,
            provider,
            format_family,
            parser,
            data_url,
            https_url,
            parse_success,
            parse_error_type,
            parse_error_message,
            parse_error_traceback,
            dataset_success,
            dataset_error_type,
            dataset_error_message,
            dataset_error_traceback,
        ) = row

        # Skip rows with no URL (can't reproduce without one).
        if not data_url:
            continue

        # Determine failing phase.
        if not parse_success and parse_error_type:
            row_phase: Literal["parse", "dataset"] = "parse"
            error_type = parse_error_type or ""
            error_message = parse_error_message or ""
            error_traceback = parse_error_traceback
        elif dataset_success is False and dataset_error_type:
            row_phase = "dataset"
            error_type = dataset_error_type or ""
            error_message = dataset_error_message or ""
            error_traceback = dataset_error_traceback
        else:
            # Both phases passed or no clear failure - skip.
            continue

        # Phase filter.
        if phase is not None and row_phase != phase:
            continue

        # Taxonomy bucket.
        row_bucket = classify(error_type, error_message).value

        # Bucket filter.
        if bucket is not None and row_bucket != bucket:
            continue

        results.append(
            FailureRow(
                collection_concept_id=coll_id or "",
                granule_concept_id=gran_id or "",
                daac=daac,
                provider=provider,
                format_family=format_family,
                parser=parser,
                data_url=data_url,
                https_url=https_url,
                phase=row_phase,
                error_type=error_type,
                error_message=error_message,
                error_traceback=error_traceback,
                bucket=row_bucket,
            )
        )

        if len(results) >= limit:
            break

    return results
