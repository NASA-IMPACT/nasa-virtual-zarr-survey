"""Generate self-contained reproducer scripts for failing granules.

Given a collection or granule concept ID (or a failure-bucket filter), queries
the survey results, picks matching failures, and emits a minimal Python script
that reproduces the error outside the survey harness.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse


@dataclass
class FailureRow:
    collection_concept_id: str
    granule_concept_id: str
    daac: str | None
    provider: str | None
    format_family: str | None
    parser: str | None
    data_url: str
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


def _registry_key(url: str) -> str:
    """Return the ``scheme://netloc`` prefix used as the ObjectStoreRegistry key."""
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    return f"{scheme}://{netloc}"


def _s3_bucket(url: str) -> str:
    """Extract the S3 bucket name from an ``s3://bucket/...`` URL."""
    parsed = urlparse(url)
    return parsed.netloc


def _indent(text: str, prefix: str = "    ") -> str:
    """Indent every line of *text* by *prefix*."""
    return textwrap.indent(text, prefix)


def generate_script(row: FailureRow) -> str:  # noqa: C901  (acceptable complexity)
    """Render a self-contained Python script that reproduces *row*'s failure."""
    url = row.data_url
    is_s3 = url.startswith("s3://")

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
            f"Original error observed in survey run:\n"
            f"    {row.error_type}: {row.error_message}\n"
            f"{tb_section}"
            f'"""\n'
            f"# This format family ({row.format_family!r}) has no registered parser in\n"
            f"# the survey as of the run date. There is no automatic reproducer that can\n"
            f"# be generated for a 'NoParserAvailable' failure.\n"
            f"#\n"
            f"# URL: {url}\n"
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
        f"Original error observed in survey run:\n"
        f"    {row.error_type}: {row.error_message}\n"
        f"{tb_section}"
        f"\n"
        f"Run with:\n"
        f"    uv run python {script_name}\n"
        f'"""\n'
    )

    # ------------------------------------------------------------------
    # Store construction block
    # ------------------------------------------------------------------
    if is_s3:
        bucket_name = _s3_bucket(url)
        store_import = "from obstore.store import S3Store"
        store_construction = (
            f"creds = earthaccess.get_s3_credentials(provider={row.provider!r})\n"
            f"    from obstore.store import S3Store\n"
            f"    store = S3Store(\n"
            f"        bucket={bucket_name!r},\n"
            f'        access_key_id=creds["accessKeyId"],\n'
            f'        secret_access_key=creds["secretAccessKey"],\n'
            f'        session_token=creds["sessionToken"],\n'
            f'        region="us-west-2",\n'
            f"    )"
        )
    else:
        store_import = "from obstore.store import HTTPStore"
        store_construction = (
            f'token_dict = getattr(earthaccess.__auth__, "token", None) or {{}}\n'
            f'    token = token_dict.get("access_token")\n'
            f"    from obstore.store import HTTPStore\n"
            f"    store = HTTPStore.from_url(\n"
            f"        {registry_key!r},\n"
            f'        client_options={{"default_headers": {{"Authorization": f"Bearer {{token}}"}}}},\n'
            f"    )"
        )

    # ------------------------------------------------------------------
    # Main body
    # ------------------------------------------------------------------
    if row.phase == "parse":
        call_lines = (
            "    manifest_store = parser(url=url, registry=registry)\n"
            '    print("Parse succeeded. Manifest store:", manifest_store)\n'
        )
    else:
        call_lines = (
            "    manifest_store = parser(url=url, registry=registry)\n"
            "    ds = manifest_store.to_virtual_dataset()\n"
            '    print("Dataset construction succeeded:")\n'
            "    print(ds)\n"
        )

    body = (
        f"def main() -> None:\n"
        f'    earthaccess.login(strategy="netrc")\n'
        f"    {store_construction}\n"
        f"\n"
        f"    url = {url!r}\n"
        f"    registry = ObjectStoreRegistry({{{registry_key!r}: store}})\n"
        f"\n"
        f"    parser = {parser_construction}\n"
        f"{call_lines}"
        f"\n"
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
    else:
        join_fragment = ""
        provider_col = "NULL"
        url_col = "NULL"

    query = f"""
        SELECT
            r.collection_concept_id,
            r.granule_concept_id,
            r.daac,
            {provider_col} AS provider,
            r.format_family,
            r.parser,
            {url_col} AS data_url,
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
