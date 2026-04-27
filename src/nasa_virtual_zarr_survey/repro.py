"""Generate self-contained reproducer scripts for failing granules.

Given a collection or granule concept ID (or a failure-bucket filter), queries
the survey results, picks matching failures, and emits a minimal Python script
that reproduces the error outside the survey harness.

The emitted script imports :func:`attempt_one` from the survey package and
calls it directly, so the reproduction executes the *same* code path as the
survey: same parser dispatch, same timeout discipline, same override
application. When :mod:`attempt` changes, repro scripts follow without
manual edits.
"""

from __future__ import annotations

import textwrap
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from nasa_virtual_zarr_survey.formats import FormatFamily
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


def _render_kwargs(kw: Mapping[str, Any]) -> str:
    """Render a kwargs dict as the inside of a function call: 'a=1, b="x"'."""
    if not kw:
        return ""
    return ", ".join(f"{k}={v!r}" for k, v in kw.items())


def _format_url_lines(row: FailureRow) -> str:
    """Return the URL block (one or two lines) shown in a repro docstring."""
    lines = [f"URL: {row.data_url}"]
    if row.https_url and row.https_url != row.data_url:
        lines.append(
            f"Download URL (HTTPS, EDL bearer token required): {row.https_url}"
        )
    return "\n".join(lines)


_DUAL_USE_BLURB = (
    "This script is also a working starting point for non-debugging "
    "virtualization workflows: edit the override kwargs (or strip the "
    "failure-context docstring) and treat it as a runnable seed. "
    "For structural inspection, run "
    "``nasa-virtual-zarr-survey probe <granule-or-collection-id>``."
)


# Format families with no parser dispatch in attempt.dispatch_parser. Treated
# as a "no parser available" reproducer (a documentation stub) — running
# attempt_one would itself record NoParserAvailable, so we surface that fact
# at gen time instead of at runtime.
_NO_DISPATCH_FAMILIES = {"HDF4"}


def generate_script(
    row: FailureRow,
    override: CollectionOverride | None = None,
) -> str:
    """Render a self-contained Python script that reproduces *row*'s failure.

    The script imports ``attempt_one`` from the survey package and calls it
    with the same inputs the survey used. This guarantees the reproducer
    executes the same code path — parser dispatch, timeout, override
    application, fingerprint extraction — as the survey itself.
    """
    url = row.data_url
    is_s3 = url.startswith("s3://")
    url_lines = _format_url_lines(row)
    script_name = f"repro_{row.granule_concept_id}.py"

    # ------------------------------------------------------------------
    # No-parser case: the format family has no dispatch. Document only.
    # ------------------------------------------------------------------
    if (
        row.parser is None
        or row.error_type == "NoParserAvailable"
        or (row.format_family in _NO_DISPATCH_FAMILIES)
    ):
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
    # Resolve format family. attempt_one needs a FormatFamily, not a parser
    # class name. If the row's format_family doesn't round-trip, fall back
    # to the documentation stub.
    # ------------------------------------------------------------------
    try:
        family = (
            FormatFamily(row.format_family) if row.format_family is not None else None
        )
    except ValueError:
        family = None
    if family is None:
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
            f"# TODO: format family {row.format_family!r} doesn't map to a known\n"
            f"# FormatFamily enum value. Cannot generate a reproducer automatically.\n"
            f"raise NotImplementedError(\n"
            f'    "Unknown format family {row.format_family!r}. "\n'
            f'    "Cannot generate a reproducer automatically."\n'
            f")\n"
        )

    registry_key = _registry_key(url)

    ov = override or CollectionOverride()
    parser_kwargs_str = _render_kwargs(ov.parser_kwargs)
    dataset_kwargs_str = _render_kwargs(ov.dataset_kwargs)
    datatree_kwargs_str = _render_kwargs(ov.datatree_kwargs)

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

    # Build a CollectionOverride literal only when the override is non-empty
    # — keeps no-override scripts free of unused machinery.
    has_override = bool(
        parser_kwargs_str or dataset_kwargs_str or datatree_kwargs_str or ov.notes
    )
    override_lines: list[str] = []
    if has_override:
        override_lines.append("    override = CollectionOverride(")
        if parser_kwargs_str:
            override_lines.append(f"        parser_kwargs=dict({parser_kwargs_str}),")
        if dataset_kwargs_str:
            override_lines.append(f"        dataset_kwargs=dict({dataset_kwargs_str}),")
        if datatree_kwargs_str:
            override_lines.append(
                f"        datatree_kwargs=dict({datatree_kwargs_str}),"
            )
        if ov.skip_dataset:
            override_lines.append("        skip_dataset=True,")
        if ov.skip_datatree:
            override_lines.append("        skip_datatree=True,")
        if ov.notes:
            override_lines.append(f"        notes={ov.notes!r},")
        override_lines.append("    )")
        override_block = "\n".join(override_lines) + "\n"
        override_arg = "override=override"
    else:
        override_block = ""
        override_arg = "override=None"

    body = (
        f"def main() -> None:\n"
        f"{argparse_block}"
        f"\n"
        f"{store_block}"
        f"\n"
        f"{cache_block}"
        f"\n"
        f"    url = {url!r}\n"
        f"    family = FormatFamily({family.value!r})\n"
        f"\n"
        f"{override_block}"
        f"    result = attempt_one(\n"
        f"        url=url,\n"
        f"        family=family,\n"
        f"        store=store,\n"
        f"        collection_concept_id={row.collection_concept_id!r},\n"
        f"        granule_concept_id={row.granule_concept_id!r},\n"
        f"        daac={row.daac!r},\n"
        f"        {override_arg},\n"
        f"    )\n"
        f"\n"
        f"    if result.success:\n"
        f'        print("Survey path passed for this granule:")\n'
        f"        print(result)\n"
        f"        return\n"
        f"\n"
        f'    print("Survey path failed for this granule:")\n'
        f"    if not result.parse_success:\n"
        f'        print(f"  parse: {{result.parse_error_type}}: '
        f'{{result.parse_error_message}}")\n'
        f"    if result.dataset_success is False:\n"
        f'        print(f"  dataset: {{result.dataset_error_type}}: '
        f'{{result.dataset_error_message}}")\n'
        f"    if result.datatree_success is False:\n"
        f'        print(f"  datatree: {{result.datatree_error_type}}: '
        f'{{result.datatree_error_message}}")\n'
        f"    raise SystemExit(1)\n"
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
        + "from obspec_utils.registry import ObjectStoreRegistry  # noqa: F401\n"
        + "\n"
        + "from nasa_virtual_zarr_survey.attempt import attempt_one\n"
        + "from nasa_virtual_zarr_survey.formats import FormatFamily\n"
        + "from nasa_virtual_zarr_survey.overrides import CollectionOverride"
        + ("  # noqa: F401\n" if not has_override else "\n")
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
    recover ``data_url`` and ``provider``. Applies the taxonomy classifier to
    each row and filters by *bucket* and/or *phase* as requested.

    Returns up to *limit* ``FailureRow`` objects.
    """
    import duckdb

    from nasa_virtual_zarr_survey.taxonomy import classify

    results_dir = Path(results_dir)
    db_path = Path(db_path)

    glob = str(results_dir / "**" / "*.parquet")

    if not list(results_dir.glob("**/*.parquet")):
        return []

    con = duckdb.connect(":memory:")

    has_db = db_path.exists()
    if has_db:
        con.execute(f"ATTACH '{db_path}' AS survey (READ_ONLY)")

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

        if not data_url:
            continue

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
            continue

        if phase is not None and row_phase != phase:
            continue

        row_bucket = classify(error_type, error_message).value

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
