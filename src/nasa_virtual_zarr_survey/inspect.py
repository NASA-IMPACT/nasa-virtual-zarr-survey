"""Format-aware structural inspector used inside generated repro scripts.

Dispatches on FormatFamily and prints both a human-readable dump and a
JSON blob fenced with sentinel lines. Used by the repro script before
the failing virtualization attempt so the operator (or any downstream
reader) can see the file's structure alongside the traceback.
"""

from __future__ import annotations

import io
import json
import traceback
from typing import Any, Callable
from urllib.parse import urlparse

from nasa_virtual_zarr_survey.formats import FormatFamily

INSPECT_JSON_BEGIN = "<<<INSPECT_JSON_BEGIN>>>"
INSPECT_JSON_END = "<<<INSPECT_JSON_END>>>"


def _emit(payload: dict[str, Any], human: str = "") -> None:
    if human:
        print(human)
    print(INSPECT_JSON_BEGIN)
    print(json.dumps(payload, indent=2, default=str))
    print(INSPECT_JSON_END)


_DISPATCH: dict[FormatFamily, Callable[..., dict[str, Any]]] = {}


def register(
    *families: FormatFamily,
) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
    def decorator(fn: Callable[..., dict[str, Any]]) -> Callable[..., dict[str, Any]]:
        for fam in families:
            _DISPATCH[fam] = fn
        return fn

    return decorator


def inspect_url(*, url: str, family: FormatFamily, store: Any) -> None:
    """Dump the structure of ``url`` (read via ``store``) to stdout.

    Always prints both a human-readable section and a JSON blob fenced by
    INSPECT_JSON_BEGIN / INSPECT_JSON_END sentinels. On error, the JSON
    payload includes ``error_type`` / ``error_message`` so the structure
    block is never missing entirely.
    """
    fn = _DISPATCH.get(family)
    if fn is None:
        _emit(
            {
                "family": family.value,
                "url": url,
                "supported": False,
                "reason": f"no inspector registered for {family.value}",
            },
            human=f"# inspect: no inspector registered for {family.value}",
        )
        return
    try:
        payload = fn(url=url, store=store)
        human = payload.pop("_human", "")
        payload["family"] = family.value
        payload["url"] = url
        payload["supported"] = True
        _emit(payload, human=human)
    except Exception as exc:
        _emit(
            {
                "family": family.value,
                "url": url,
                "supported": True,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
            human=(
                f"# inspect: {type(exc).__name__}: {exc}\n" + traceback.format_exc()
            ),
        )


def _read_bytes(url: str, store: Any) -> bytes:
    """Read the full object body for ``url``, handling file:// vs object stores."""
    parsed = urlparse(url)
    if parsed.scheme == "file":
        with open(parsed.path, "rb") as f:
            return f.read()
    path = parsed.path.lstrip("/")
    return store.get(path).bytes()


@register(FormatFamily.HDF5, FormatFamily.NETCDF4)
def _inspect_hdf5(*, url: str, store: Any) -> dict[str, Any]:
    import h5py

    parsed = urlparse(url)
    if parsed.scheme == "file":
        opener: Any = h5py.File(parsed.path, "r")
    else:
        # h5py needs a seekable file-like; pull bytes once for inspection.
        opener = h5py.File(io.BytesIO(_read_bytes(url, store)), "r")

    with opener as f:
        groups: dict[str, dict[str, Any]] = {
            "/": {
                "datasets": [],
                "attrs": {k: str(v) for k, v in list(f.attrs.items())[:10]},
            }
        }

        def visitor(name: str, obj: Any) -> None:
            full = "/" + name
            if isinstance(obj, h5py.Group):
                groups.setdefault(
                    full,
                    {
                        "datasets": [],
                        "attrs": {k: str(v) for k, v in list(obj.attrs.items())[:10]},
                    },
                )
            elif isinstance(obj, h5py.Dataset):
                if "/" in name:
                    parent = "/" + name.rsplit("/", 1)[0]
                else:
                    parent = "/"
                bucket = groups.setdefault(parent, {"datasets": [], "attrs": {}})
                fillvalue = obj.fillvalue
                if hasattr(fillvalue, "item"):
                    try:
                        fillvalue = fillvalue.item()
                    except (ValueError, TypeError):
                        fillvalue = str(fillvalue)
                bucket["datasets"].append(
                    {
                        "name": name.rsplit("/", 1)[-1],
                        "shape": list(obj.shape),
                        "dtype": str(obj.dtype),
                        "chunks": list(obj.chunks) if obj.chunks else None,
                        "compression": obj.compression,
                        "compression_opts": obj.compression_opts,
                        "fillvalue": fillvalue,
                        "attrs": {k: str(v) for k, v in list(obj.attrs.items())[:10]},
                    }
                )

        f.visititems(visitor)
        root_attrs = groups["/"]["attrs"]
        human_lines = ["# HDF5 inspection"]
        for g, b in sorted(groups.items()):
            human_lines.append(f"  {g}: {len(b['datasets'])} dataset(s)")
        return {
            "groups": groups,
            "root_attrs": root_attrs,
            "_human": "\n".join(human_lines),
        }


@register(FormatFamily.ZARR)
def _inspect_zarr(*, url: str, store: Any) -> dict[str, Any]:
    import zarr
    import zarr.storage

    parsed = urlparse(url)
    if parsed.scheme == "file":
        zstore: Any = zarr.storage.LocalStore(parsed.path)
    else:
        zstore = zarr.storage.ObjectStore(store)

    root = zarr.open(store=zstore, mode="r")
    arrays: list[dict[str, Any]] = []

    def walk(node: Any, prefix: str = "") -> None:
        for name, child in node.members():
            full = f"{prefix}/{name}"
            if isinstance(child, zarr.Array):
                meta = child.metadata
                codecs = [type(c).__name__ for c in getattr(meta, "codecs", []) or []]
                arrays.append(
                    {
                        "name": name,
                        "path": full,
                        "shape": list(child.shape),
                        "dtype": str(child.dtype),
                        "chunks": list(child.chunks),
                        "codecs": codecs,
                        "fillvalue": getattr(meta, "fill_value", None),
                    }
                )
            elif isinstance(child, zarr.Group):
                walk(child, full)

    if isinstance(root, zarr.Group):
        walk(root)
    elif isinstance(root, zarr.Array):
        arrays.append(
            {
                "name": "",
                "path": "/",
                "shape": list(root.shape),
                "dtype": str(root.dtype),
                "chunks": list(root.chunks),
                "codecs": [
                    type(c).__name__ for c in getattr(root.metadata, "codecs", []) or []
                ],
                "fillvalue": getattr(root.metadata, "fill_value", None),
            }
        )

    human_lines = ["# Zarr inspection"]
    for a in arrays:
        human_lines.append(f"  {a['path']}: shape={a['shape']} dtype={a['dtype']}")
    return {"arrays": arrays, "_human": "\n".join(human_lines)}


@register(FormatFamily.NETCDF3)
def _inspect_netcdf3(*, url: str, store: Any) -> dict[str, Any]:
    from scipy.io import netcdf_file

    parsed = urlparse(url)
    if parsed.scheme == "file":
        path_or_buf: Any = parsed.path
    else:
        path_or_buf = io.BytesIO(_read_bytes(url, store))

    with netcdf_file(path_or_buf, "r") as f:
        dims = {k: int(v) for k, v in f.dimensions.items()}
        variables: list[dict[str, Any]] = []
        for name, var in f.variables.items():
            try:
                dtype = str(var.data.dtype)
            except AttributeError:
                dtype = "?"
            variables.append(
                {
                    "name": name,
                    "dimensions": list(var.dimensions),
                    "shape": list(var.shape),
                    "dtype": dtype,
                    "attrs": {
                        k: str(v)
                        for k, v in dict(var._attributes).items()
                        if not k.startswith("_")
                    },
                }
            )
        global_attrs = {
            k: str(v) for k, v in dict(f._attributes).items() if not k.startswith("_")
        }

    human = (
        f"# NetCDF3 inspection\n  dims: {dims}\n  "
        f"vars: {[v['name'] for v in variables]}"
    )
    return {
        "dimensions": dims,
        "variables": variables,
        "global_attrs": global_attrs,
        "_human": human,
    }


@register(FormatFamily.GEOTIFF)
def _inspect_geotiff(*, url: str, store: Any) -> dict[str, Any]:
    import tifffile

    parsed = urlparse(url)
    if parsed.scheme == "file":
        src: Any = parsed.path
    else:
        src = io.BytesIO(_read_bytes(url, store))

    ifds: list[dict[str, Any]] = []
    with tifffile.TiffFile(src) as tif:
        for i, page in enumerate(tif.pages):
            tile = getattr(page, "tile", None)
            predictor = getattr(page, "predictor", None)
            photometric = getattr(page, "photometric", None)
            tags = getattr(page, "tags", None)
            compression = page.compression
            # tifffile exposes compression as a COMPRESSION enum; .name renders
            # as e.g. "ADOBE_DEFLATE" rather than the raw int (8).
            compression_str = getattr(compression, "name", str(compression))
            predictor_str = getattr(predictor, "name", None) if predictor else None
            photometric_str = (
                getattr(photometric, "name", str(photometric))
                if photometric is not None
                else None
            )
            ifds.append(
                {
                    "index": i,
                    "shape": list(page.shape),
                    "dtype": str(page.dtype),
                    "tile": list(tile) if tile else None,
                    "compression": compression_str,
                    "predictor": predictor_str,
                    "photometric": photometric_str,
                    "tags": (
                        {tag.name: str(tag.value)[:120] for tag in list(tags)[:20]}
                        if tags is not None
                        else {}
                    ),
                }
            )
    return {"ifds": ifds, "_human": f"# GeoTIFF inspection: {len(ifds)} IFD(s)"}


@register(FormatFamily.DMRPP)
def _inspect_dmrpp(*, url: str, store: Any) -> dict[str, Any]:
    # stdlib expat (3.7.1+) blocks billion-laughs / quadratic-blowup. DMR++
    # files have no external entity references, so XXE-class attacks (which
    # defusedxml additionally guards against) are not a relevant threat here.
    import xml.etree.ElementTree as ET

    data = _read_bytes(url, store)
    root = ET.fromstring(data)
    DAP4 = "{http://xml.opendap.org/ns/DAP/4.0#}"
    DMRPP = "{http://xml.opendap.org/dap/dmrpp/1.0.0#}"
    DTYPES = (
        "Float32",
        "Float64",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Byte",
        "String",
    )

    variables: list[dict[str, Any]] = []
    for child in root.iter():
        tag = child.tag
        if tag.startswith(DAP4):
            tag = tag[len(DAP4) :]
        if tag in DTYPES:
            dims = [
                {
                    "name": d.attrib.get("name"),
                    "size": int(d.attrib.get("size", 0)),
                }
                for d in child.findall(f"{DAP4}Dim")
            ]
            chunks_el = child.find(f"{DMRPP}chunks")
            compression = (
                chunks_el.attrib.get("compressionType")
                if chunks_el is not None
                else None
            )
            variables.append(
                {
                    "name": child.attrib.get("name"),
                    "dtype": tag,
                    "dims": dims,
                    "compression": compression,
                }
            )
    return {
        "variables": variables,
        "_human": f"# DMR++ inspection: {len(variables)} variable(s)",
    }


@register(FormatFamily.FITS)
def _inspect_fits(*, url: str, store: Any) -> dict[str, Any]:
    from astropy.io import fits

    parsed = urlparse(url)
    if parsed.scheme == "file":
        src: Any = parsed.path
    else:
        src = io.BytesIO(_read_bytes(url, store))

    hdus: list[dict[str, Any]] = []
    with fits.open(src) as hdul:
        for i, hdu in enumerate(hdul):
            shape = list(hdu.data.shape) if hdu.data is not None else []
            dtype = str(hdu.data.dtype) if hdu.data is not None else None
            header = {k: str(v) for k, v in list(hdu.header.items())[:20]}
            hdus.append(
                {
                    "index": i,
                    "name": hdu.name,
                    "type": type(hdu).__name__,
                    "shape": shape,
                    "dtype": dtype,
                    "header": header,
                }
            )
    return {"hdus": hdus, "_human": f"# FITS inspection: {len(hdus)} HDU(s)"}
