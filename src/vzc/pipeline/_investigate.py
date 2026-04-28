"""Unified script generator: ``investigate`` (replaces ``probe`` + ``repro``).

One concept ID in, one runnable Python script out. Two modes:

- **virtual** (default): emits the survey's own VirtualiZarr code path
  (``parser → manifest_store → to_virtual_dataset/datatree``) for stepping
  through a parser- or xarray-level failure.
- **native**: emits an exploration script using the format-appropriate
  library (``h5py`` for HDF5/NetCDF4, ``netCDF4`` for NetCDF3, ``astropy``
  for FITS, ``zarr`` for Zarr, ``tifffile`` for GeoTIFF) for format triage
  independent of any virtualization.

Resolves ``concept_id`` against the local ``state.json`` first; falls back
to one or two CMR calls when the ID isn't in state.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from vzc._config import (
    AccessMode,
    DEFAULT_OVERRIDES_PATH,
    DEFAULT_STATE_PATH,
)
from vzc.pipeline._overrides import CollectionOverride, OverrideRegistry
from vzc.pipeline._probe import (
    ProbeTarget,
    generate_script as _generate_native_script,
    resolve_target,
)
from vzc.pipeline._scripts import (
    _registry_key,
    render_cache_argparse,
    render_cache_wiring,
    render_login_and_store,
)


def investigate(
    concept_id: str,
    *,
    mode: Literal["virtual", "native"] = "virtual",
    access: AccessMode = "external",
) -> str:
    """Return the source of a runnable Python script for ``concept_id``.

    Pipe the result to ``uv run python -`` to execute now, or write to a
    file for later iteration.

    ``concept_id`` may be a collection (``C...``) or granule (``G...``).
    ``mode="virtual"`` emits the survey's parser → dataset/datatree path;
    ``mode="native"`` emits a format-aware exploration script. ``access``
    selects which URL flavour the script binds to (S3 vs HTTPS).

    Reads ``output/state.json`` and ``config/collection_overrides.toml``
    (relative to cwd) to resolve the URL and bake in matching overrides.
    """
    target = resolve_target(DEFAULT_STATE_PATH, concept_id, access)
    if mode == "native":
        return _generate_native_script(target)
    override = _override_for_target(target)
    return _generate_virtual_script(target, override)


def _override_for_target(target: ProbeTarget) -> CollectionOverride | None:
    if target.collection_concept_id is None:
        return None
    p = Path(DEFAULT_OVERRIDES_PATH)
    if not p.exists():
        return None
    return OverrideRegistry.from_toml(p).for_collection(target.collection_concept_id)


def _generate_virtual_script(
    target: ProbeTarget, override: CollectionOverride | None
) -> str:
    """Render a script that imports ``attempt_one`` and runs the survey path.

    Mirrors the previous ``repro`` output but takes a :class:`ProbeTarget`
    (no FailureRow needed): the script is a runnable seed for stepping
    through the survey's behaviour against the resolved URL + format
    family. The reader can edit kwargs or override fields and re-run.
    """
    family = target.sniffed_family
    script_name = (
        f"investigate_{target.granule_concept_id or target.collection_concept_id}.py"
    )

    if family is None or family.value == "HDF4":
        return _no_dispatch_stub(target, script_name)

    url = target.data_url
    is_s3 = url.startswith("s3://")
    registry_key = _registry_key(url)
    docstring = _virtual_docstring(target, script_name)

    store_block = render_login_and_store(
        url=url, provider=target.provider, registry_key=registry_key
    )
    cache_block = render_cache_wiring(registry_key=registry_key)
    argparse_block = render_cache_argparse()
    store_import = (
        "from obstore.store import S3Store"
        if is_s3
        else "from obstore.store import HTTPStore"
    )

    ov = override or CollectionOverride()
    has_override = bool(
        ov.parser_kwargs
        or ov.dataset_kwargs
        or ov.datatree_kwargs
        or ov.skip_dataset
        or ov.skip_datatree
        or ov.notes
    )
    override_block = _override_literal(ov) if has_override else ""
    override_arg = "override=override" if has_override else "override=None"

    body = (
        "def main() -> None:\n"
        f"{argparse_block}"
        "\n"
        f"{store_block}"
        "\n"
        f"{cache_block}"
        "\n"
        f"    url = {url!r}\n"
        f"    family = FormatFamily({family.value!r})\n"
        "\n"
        f"{override_block}"
        f"    result = attempt_one(\n"
        f"        url=url,\n"
        f"        family=family,\n"
        f"        store=store,\n"
        f"        collection_concept_id={target.collection_concept_id!r},\n"
        f"        granule_concept_id={target.granule_concept_id!r},\n"
        f"        daac={target.daac!r},\n"
        f"        {override_arg},\n"
        f"    )\n"
        "\n"
        "    if result.success:\n"
        '        print("Survey path passed for this granule:")\n'
        "        print(result)\n"
        "        return\n"
        "\n"
        '    print("Survey path failed for this granule:")\n'
        "    if not result.parse_success:\n"
        '        print(f"  parse: {result.parse_error_type}: '
        '{result.parse_error_message}")\n'
        "    if result.dataset_success is False:\n"
        '        print(f"  dataset: {result.dataset_error_type}: '
        '{result.dataset_error_message}")\n'
        "    if result.datatree_success is False:\n"
        '        print(f"  datatree: {result.datatree_error_type}: '
        '{result.datatree_error_message}")\n'
        "    raise SystemExit(1)\n"
        "\n"
        'if __name__ == "__main__":\n'
        "    main()\n"
    )

    return (
        docstring
        + "from __future__ import annotations\n\n"
        + "import earthaccess\n"
        + f"{store_import}\n"
        + "from obspec_utils.registry import ObjectStoreRegistry  # noqa: F401\n"
        + "\n"
        + "from vzc.pipeline._attempt import attempt_one\n"
        + "from vzc.core.formats import FormatFamily\n"
        + "from vzc.pipeline._overrides import CollectionOverride"
        + ("  # noqa: F401\n" if not has_override else "\n")
        + "\n\n"
        + body
    )


def _virtual_docstring(target: ProbeTarget, script_name: str) -> str:
    download_line = ""
    return (
        f'"""Investigate {target.collection_concept_id or "?"} / '
        f"{target.granule_concept_id or '?'} (virtual mode).\n"
        f"\n"
        f"DAAC: {target.daac}\n"
        f"Format family: {target.sniffed_family.value if target.sniffed_family else '(unknown)'}\n"
        f"\n"
        f"URL: {target.data_url}\n"
        f"{download_line}"
        f"\n"
        f"Reproduces the survey's VirtualiZarr code path. Edit override kwargs\n"
        f"or strip this docstring to use as a runnable seed.\n"
        f"For format-aware structural inspection instead, re-run with\n"
        f"``investigate {target.collection_concept_id or target.granule_concept_id} --mode native``.\n"
        f"\n"
        f"Run with:\n"
        f"    uv run python {script_name}\n"
        f'"""\n'
    )


def _no_dispatch_stub(target: ProbeTarget, script_name: str) -> str:
    family_repr = target.sniffed_family.value if target.sniffed_family else None
    return (
        f'"""Investigate {target.collection_concept_id or "?"} / '
        f"{target.granule_concept_id or '?'} (virtual mode).\n"
        f"\n"
        f"DAAC: {target.daac}\n"
        f"Format family: {family_repr}\n"
        f"\n"
        f"URL: {target.data_url}\n"
        f"\n"
        f"No VirtualiZarr parser is registered for this format family, so the\n"
        f"virtual path cannot run. Use ``investigate ... --mode native`` to\n"
        f"explore the file with a format-appropriate library.\n"
        f'"""\n'
        f"raise NotImplementedError(\n"
        f'    "No parser is registered for format family {family_repr!r}. "\n'
        f'    "Cannot reproduce automatically; use --mode native instead."\n'
        f")\n"
    )


def _override_literal(ov: CollectionOverride) -> str:
    """Render a ``CollectionOverride(...)`` literal for non-empty overrides."""
    lines = ["    override = CollectionOverride("]
    if ov.parser_kwargs:
        kw = ", ".join(f"{k}={v!r}" for k, v in ov.parser_kwargs.items())
        lines.append(f"        parser_kwargs=dict({kw}),")
    if ov.dataset_kwargs:
        kw = ", ".join(f"{k}={v!r}" for k, v in ov.dataset_kwargs.items())
        lines.append(f"        dataset_kwargs=dict({kw}),")
    if ov.datatree_kwargs:
        kw = ", ".join(f"{k}={v!r}" for k, v in ov.datatree_kwargs.items())
        lines.append(f"        datatree_kwargs=dict({kw}),")
    if ov.skip_dataset:
        lines.append("        skip_dataset=True,")
    if ov.skip_datatree:
        lines.append("        skip_datatree=True,")
    if ov.notes:
        lines.append(f"        notes={ov.notes!r},")
    lines.append("    )")
    return "\n".join(lines) + "\n"
