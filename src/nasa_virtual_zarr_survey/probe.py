"""Generate self-contained probe scripts for investigating CMR collections / granules.

``probe`` is the diagnostic counterpart to ``repro``. ``repro`` reproduces a
failure the survey already observed; ``probe`` investigates a concept ID
regardless of survey state — most importantly collections that were skipped
at discover time (``skip_reason='format_unknown'``, no granules attempted,
nothing in the Parquet log).

The output is a runnable Python script that fetches the collection / granule
UMM-JSON, prints both ``direct`` and ``external`` data links, constructs an
appropriate obstore-backed store, and (when format can be sniffed) calls
``inspect.inspect_url`` for a structural dump.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import click

from nasa_virtual_zarr_survey.formats import FormatFamily, classify_format
from nasa_virtual_zarr_survey.script_template import (
    _registry_key,
    render_cache_argparse,
    render_cache_wiring,
    render_earthaccess_login,
    render_inspect_block,
    render_store,
)


@dataclass
class ProbeTarget:
    """Resolved inputs for ``generate_script``.

    ``source`` records whether the URL came from the local DB ("db") or a CMR
    call at gen time ("cmr"). It feeds the docstring's "CMR was/was not
    called at gen time" note.
    """

    kind: Literal["collection", "granule"]
    collection_concept_id: str | None
    granule_concept_id: str | None
    data_url: str
    provider: str | None
    sniffed_family: FormatFamily | None
    daac: str | None
    source: Literal["db", "cmr"]


# ---------------------------------------------------------------------------
# Resolve target
# ---------------------------------------------------------------------------


def _kind_from_id(concept_id: str) -> Literal["collection", "granule"]:
    if concept_id.startswith("C"):
        return "collection"
    if concept_id.startswith("G"):
        return "granule"
    raise click.UsageError(
        f"concept ID {concept_id!r} must start with 'C' (collection) or 'G' (granule)"
    )


def _granule_format_from_umm(g: Any) -> str | None:
    """Pull a Format string from a DataGranule UMM-JSON envelope."""
    try:
        info = g["umm"]["DataGranule"]["ArchiveAndDistributionInformation"]
    except (KeyError, TypeError):
        return None
    if isinstance(info, list):
        for entry in info:
            if isinstance(entry, dict):
                fmt = entry.get("Format")
                if fmt:
                    return fmt
    elif isinstance(info, dict):
        return info.get("Format")
    return None


def _resolve_granule_from_db(
    con: Any, granule_concept_id: str, access: str
) -> ProbeTarget | None:
    row = con.execute(
        """
        SELECT g.collection_concept_id, g.granule_concept_id, g.data_url,
               c.provider, c.format_family, c.daac, c.format_declared,
               g.access_mode
        FROM granules g
        LEFT JOIN collections c ON c.concept_id = g.collection_concept_id
        WHERE g.granule_concept_id = ? AND g.access_mode = ?
        ORDER BY g.stratification_bin
        LIMIT 1
        """,
        [granule_concept_id, access],
    ).fetchone()
    if row is None:
        return None
    (coll_id, gran_id, data_url, provider, fam_str, daac, fmt_declared, _mode) = row
    if data_url is None:
        return None
    family = _coerce_family(fam_str) or classify_format(fmt_declared, data_url)
    return ProbeTarget(
        kind="granule",
        collection_concept_id=coll_id,
        granule_concept_id=gran_id,
        data_url=data_url,
        provider=provider,
        sniffed_family=family,
        daac=daac,
        source="db",
    )


def _resolve_granule_from_cmr(granule_concept_id: str, access: str) -> ProbeTarget:
    import earthaccess

    results = earthaccess.search_data(concept_id=granule_concept_id, count=1)
    if not results:
        raise click.UsageError(
            f"concept ID {granule_concept_id!r} not found in survey DB or CMR"
        )
    g = results[0]
    links = g.data_links(access=access) or []
    if not links:
        other = "external" if access == "direct" else "direct"
        raise click.UsageError(
            f"granule {granule_concept_id!r} has no {access} data link; "
            f"try --access {other}"
        )
    data_url = links[0]
    meta = g.get("meta", {}) if hasattr(g, "get") else g["meta"]
    provider = meta.get("provider-id")
    declared = _granule_format_from_umm(g)
    family = classify_format(declared, data_url)
    return ProbeTarget(
        kind="granule",
        collection_concept_id=meta.get("collection-concept-id"),
        granule_concept_id=meta.get("concept-id", granule_concept_id),
        data_url=data_url,
        provider=provider,
        sniffed_family=family,
        daac=provider,
        source="cmr",
    )


def _resolve_collection_db_row(
    con: Any, collection_concept_id: str
) -> tuple[str | None, str | None, str | None, str | None] | None:
    """Return (provider, format_family, format_declared, daac) or None."""
    return con.execute(
        """
        SELECT provider, format_family, format_declared, daac
        FROM collections WHERE concept_id = ?
        """,
        [collection_concept_id],
    ).fetchone()


def _resolve_collection_granule_from_db(
    con: Any, collection_concept_id: str, access: str
) -> tuple[str, str] | None:
    """Return ``(granule_concept_id, data_url)`` from sampled granules, lowest bin first."""
    row = con.execute(
        """
        SELECT granule_concept_id, data_url
        FROM granules
        WHERE collection_concept_id = ? AND access_mode = ? AND data_url IS NOT NULL
        ORDER BY stratification_bin
        LIMIT 1
        """,
        [collection_concept_id, access],
    ).fetchone()
    if row is None or row[1] is None:
        return None
    return row[0], row[1]


def _resolve_collection_from_cmr_search_data(
    collection_concept_id: str, access: str
) -> tuple[str, str, str | None, str | None]:
    """Return ``(granule_concept_id, data_url, provider, declared_format)`` or raise."""
    import earthaccess

    results = earthaccess.search_data(concept_id=collection_concept_id, count=1)
    if not results:
        raise click.UsageError(
            f"concept ID {collection_concept_id!r} not found in survey DB or CMR"
        )
    g = results[0]
    links = g.data_links(access=access) or []
    if not links:
        other = "external" if access == "direct" else "direct"
        raise click.UsageError(
            f"collection {collection_concept_id!r} first granule has no "
            f"{access} data link; try --access {other}"
        )
    meta = g.get("meta", {}) if hasattr(g, "get") else g["meta"]
    return (
        meta.get("concept-id", ""),
        links[0],
        meta.get("provider-id"),
        _granule_format_from_umm(g),
    )


def _resolve_collection_info_from_cmr(collection_concept_id: str) -> bool:
    """``search_datasets`` probe — return True if the collection exists."""
    import earthaccess

    results = earthaccess.search_datasets(concept_id=collection_concept_id, count=1)
    return bool(results)


def _coerce_family(fam_str: str | None) -> FormatFamily | None:
    if fam_str is None:
        return None
    try:
        return FormatFamily(fam_str)
    except ValueError:
        return None


def resolve_target(
    db_path: Path,
    concept_id: str,
    access: Literal["direct", "external"],
) -> ProbeTarget:
    """Resolve a concept ID to a ``ProbeTarget``, preferring local DB.

    Fallback chain (per the plan):

    - Granule input: DB hit → return; DB miss → one ``search_data`` call.
    - Collection input: DB hit with sampled granules → return; DB hit but no
      granules → one ``search_data`` call; no DB row → ``search_datasets``
      (to confirm existence) plus ``search_data`` (for a granule).
    """
    kind = _kind_from_id(concept_id)

    has_db = Path(db_path).exists()
    con = None
    if has_db:
        from nasa_virtual_zarr_survey.db import connect, init_schema

        con = connect(db_path)
        init_schema(con)

    try:
        if kind == "granule":
            if con is not None:
                hit = _resolve_granule_from_db(con, concept_id, access)
                if hit is not None:
                    return hit
            return _resolve_granule_from_cmr(concept_id, access)

        # Collection input.
        coll_row = _resolve_collection_db_row(con, concept_id) if con else None
        if coll_row is not None:
            provider, fam_str, fmt_declared, daac = coll_row
            granule_hit = _resolve_collection_granule_from_db(con, concept_id, access)
            if granule_hit is not None:
                gran_id, data_url = granule_hit
                family = _coerce_family(fam_str) or classify_format(
                    fmt_declared, data_url
                )
                return ProbeTarget(
                    kind="collection",
                    collection_concept_id=concept_id,
                    granule_concept_id=gran_id,
                    data_url=data_url,
                    provider=provider,
                    sniffed_family=family,
                    daac=daac,
                    source="db",
                )
            # DB hit, no sampled granules — fall through to one CMR call.
            gran_id, data_url, prov_cmr, declared_cmr = (
                _resolve_collection_from_cmr_search_data(concept_id, access)
            )
            family = _coerce_family(fam_str) or classify_format(
                declared_cmr or fmt_declared, data_url
            )
            return ProbeTarget(
                kind="collection",
                collection_concept_id=concept_id,
                granule_concept_id=gran_id,
                data_url=data_url,
                provider=provider or prov_cmr,
                sniffed_family=family,
                daac=daac,
                source="cmr",
            )

        # No DB or collection not in DB: confirm existence via search_datasets,
        # then fetch a granule via search_data. Two CMR calls.
        if not _resolve_collection_info_from_cmr(concept_id):
            raise click.UsageError(
                f"concept ID {concept_id!r} not found in survey DB or CMR"
            )
        gran_id, data_url, prov_cmr, declared_cmr = (
            _resolve_collection_from_cmr_search_data(concept_id, access)
        )
        family = classify_format(declared_cmr, data_url)
        return ProbeTarget(
            kind="collection",
            collection_concept_id=concept_id,
            granule_concept_id=gran_id,
            data_url=data_url,
            provider=prov_cmr,
            sniffed_family=family,
            daac=prov_cmr,
            source="cmr",
        )
    finally:
        if con is not None:
            con.close()


# ---------------------------------------------------------------------------
# Generate script
# ---------------------------------------------------------------------------


def _docstring(target: ProbeTarget) -> str:
    if target.kind == "collection":
        title = (
            f"Probe for collection {target.collection_concept_id} "
            f"(picked granule {target.granule_concept_id})."
        )
        cmr_note = (
            "CMR was called at gen time to pick this granule."
            if target.source == "cmr"
            else "CMR was not called at gen time; granule came from the local survey DB."
        )
    else:
        title = f"Probe for granule {target.granule_concept_id}."
        cmr_note = (
            "CMR was called at gen time to look up this granule."
            if target.source == "cmr"
            else "CMR was not called at gen time; granule came from the local survey DB."
        )
    family_str = (
        target.sniffed_family.value
        if target.sniffed_family is not None
        else "(unknown)"
    )
    return (
        f'"""{title}\n'
        f"\n"
        f"DAAC: {target.daac}\n"
        f"Provider: {target.provider}\n"
        f"Picked URL: {target.data_url}\n"
        f"Access mode: bound at gen time via --access\n"
        f"Sniffed format family: {family_str}\n"
        f"\n"
        f"{cmr_note}\n"
        f'"""\n'
    )


def _render_collection_umm(collection_concept_id: str) -> str:
    return (
        "    # --- collection UMM ---\n"
        "    # Inline so the dump reflects current CMR state at run time.\n"
        f"    coll_results = earthaccess.search_datasets(concept_id={collection_concept_id!r}, count=1)\n"
        "    if coll_results:\n"
        "        print(json.dumps(dict(coll_results[0]), indent=2, default=str))\n"
        "    else:\n"
        f'        print("No collection found for {collection_concept_id}")\n'
    )


def _render_granule_umm(granule_concept_id: str) -> str:
    return (
        "    # --- granule UMM ---\n"
        f"    gran_results = earthaccess.search_data(concept_id={granule_concept_id!r}, count=1)\n"
        "    if not gran_results:\n"
        f'        raise SystemExit("No granule found for {granule_concept_id}")\n'
        "    g = gran_results[0]\n"
        "    print(json.dumps(dict(g), indent=2, default=str))\n"
        '    print("direct links:  ", g.data_links(access="direct"))\n'
        '    print("external links:", g.data_links(access="external"))\n'
        "    try:\n"
        '        info = g["umm"]["DataGranule"]["ArchiveAndDistributionInformation"]\n'
        '        print("ArchiveAndDistributionInformation:", info)\n'
        "    except (KeyError, TypeError):\n"
        "        pass\n"
    )


def _render_inspect_section(target: ProbeTarget, registry_key: str) -> str:
    """Render the ``# --- inspect ---`` section.

    When the format is sniffed: bake ``family = FormatFamily("...")`` and
    call ``inspect_url``. When it isn't: emit a fenced comment with the
    extension, plus a commented-out template line, and skip the call.
    """
    url = target.data_url
    if target.sniffed_family is not None:
        body = render_inspect_block(url=url, family=target.sniffed_family)
        return (
            "    # --- inspect ---\n"
            f"    registry = ObjectStoreRegistry({{{registry_key!r}: store}})\n"
            f"{body}"
        )
    # Format not sniffed.
    from urllib.parse import urlparse

    ext = "".join(Path(urlparse(url).path).suffixes) or "(none)"
    return (
        "    # --- inspect ---\n"
        f"    registry = ObjectStoreRegistry({{{registry_key!r}: store}})\n"
        f"    # format unknown — extension {ext!r} not in formats._EXT.\n"
        "    # Edit the next line to try a guess (e.g. FormatFamily.HDF5):\n"
        f"    # url = {url!r}\n"
        '    # family = FormatFamily("HDF5")\n'
        "    # inspect_url(url=url, family=family, store=store)\n"
    )


def generate_script(target: ProbeTarget) -> str:
    """Render a runnable probe script for ``target``."""
    url = target.data_url
    is_s3 = url.startswith("s3://")
    registry_key = _registry_key(url)

    docstring = _docstring(target)

    store_import = (
        "from obstore.store import S3Store"
        if is_s3
        else "from obstore.store import HTTPStore"
    )
    optional_inspect_imports = ""
    if target.sniffed_family is not None:
        optional_inspect_imports = (
            "from nasa_virtual_zarr_survey.inspect import inspect_url\n"
            "from nasa_virtual_zarr_survey.formats import FormatFamily\n"
        )

    imports_block = (
        "# --- imports ---\n"
        "from __future__ import annotations\n"
        "\n"
        "import json\n"
        "\n"
        "import earthaccess\n"
        f"{store_import}\n"
        "from obspec_utils.registry import ObjectStoreRegistry\n"
        f"{optional_inspect_imports}"
    )

    argparse_block = "    # --- argparse ---\n" + render_cache_argparse()

    login_block = "    # --- earthaccess login ---\n" + render_earthaccess_login()

    store_block = "    # --- store construction ---\n" + render_store(
        url=url, provider=target.provider, registry_key=registry_key
    )

    cache_block = "    # --- cache wiring (optional) ---\n" + render_cache_wiring(
        registry_key=registry_key
    )

    if target.kind == "collection":
        collection_umm = _render_collection_umm(target.collection_concept_id or "")
    else:
        collection_umm = ""

    granule_umm = _render_granule_umm(target.granule_concept_id or "")
    inspect_section = _render_inspect_section(target, registry_key)

    body = (
        "def main() -> None:\n"
        f"{argparse_block}"
        "\n"
        f"{login_block}"
        "\n"
        f"{collection_umm}" + ("\n" if collection_umm else "") + f"{granule_umm}"
        "\n"
        f"{store_block}"
        "\n"
        f"{cache_block}"
        "\n"
        f"{inspect_section}"
        "\n"
        '\nif __name__ == "__main__":\n'
        "    main()\n"
    )

    return docstring + imports_block + "\n\n" + body
