"""Virtual store feasibility analysis: fingerprint extraction and cubability checks."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import numpy as np


class CubabilityVerdict(StrEnum):
    FEASIBLE = "FEASIBLE"
    INCOMPATIBLE = "INCOMPATIBLE"
    INCONCLUSIVE = "INCONCLUSIVE"
    NOT_ATTEMPTED = "NOT_ATTEMPTED"


@dataclass
class CubabilityResult:
    verdict: CubabilityVerdict
    reason: str = ""
    concat_dim: str | None = None


def extract_fingerprint(ds: Any) -> dict[str, Any]:
    """Extract a JSON-serializable fingerprint from an xarray Dataset.

    Safe to call on any xr.Dataset; uses only metadata plus coord byte hashes.
    Never reads data_var values.
    """

    def _dtype_str(x: Any) -> str:
        return str(np.dtype(x.dtype)) if hasattr(x, "dtype") else str(type(x))

    def _codecs_from_encoding(var: Any) -> list[str]:
        enc = getattr(var, "encoding", {}) or {}
        codecs: list[str] = []
        if "compressor" in enc and enc["compressor"] is not None:
            codecs.append(type(enc["compressor"]).__name__)
        for f in enc.get("filters") or []:
            if f is not None:
                codecs.append(type(f).__name__)
        if not codecs and "codecs" in enc and enc["codecs"] is not None:
            for c in enc["codecs"]:
                codecs.append(type(c).__name__ if not isinstance(c, str) else c)
        return codecs

    def _chunks(var: Any) -> list[int] | None:
        enc = getattr(var, "encoding", {}) or {}
        ch = enc.get("chunks") or enc.get("preferred_chunks")
        if ch is None and hasattr(var, "chunks") and var.chunks is not None:
            ch = tuple(c[0] for c in var.chunks)
        return list(ch) if ch is not None else None

    def _fill_value(var: Any) -> str | None:
        enc = getattr(var, "encoding", {}) or {}
        fv = enc.get("_FillValue") or enc.get("fill_value")
        return repr(fv) if fv is not None else None

    fp: dict[str, Any] = {
        "dims": {str(k): int(v) for k, v in ds.sizes.items()},
        "data_vars": {},
        "coords": {},
    }

    for name, var in ds.data_vars.items():
        fp["data_vars"][str(name)] = {
            "dtype": _dtype_str(var),
            "dims": [str(d) for d in var.dims],
            "chunks": _chunks(var),
            "fill_value": _fill_value(var),
            "codecs": _codecs_from_encoding(var),
        }

    for name, var in ds.coords.items():
        try:
            arr = np.asarray(var.values)
        except Exception:
            arr = None
        values_hash = hashlib.sha256(arr.tobytes()).hexdigest() if arr is not None else ""
        mn = mx = None
        if arr is not None and arr.size > 0:
            try:
                flat = arr.ravel()
                sorted_flat = np.sort(flat)
                mn = _coord_endpoint(sorted_flat[0], arr.dtype)
                mx = _coord_endpoint(sorted_flat[-1], arr.dtype)
            except Exception:
                pass
        fp["coords"][str(name)] = {
            "dtype": _dtype_str(var),
            "dims": [str(d) for d in var.dims],
            "shape": [int(s) for s in arr.shape] if arr is not None else [],
            "values_hash": values_hash,
            "min": mn,
            "max": mx,
        }

    return fp


def fingerprint_to_json(fp: dict[str, Any]) -> str:
    return json.dumps(fp, default=str, sort_keys=True)


def fingerprint_from_json(s: str | None) -> dict[str, Any] | None:
    if not s:
        return None
    try:
        return json.loads(s)
    except (TypeError, ValueError):
        return None


def _variables_match(fps: list[dict]) -> tuple[bool, str]:
    names_sets = [set(fp["data_vars"].keys()) for fp in fps]
    if len(set(frozenset(s) for s in names_sets)) > 1:
        diff = names_sets[0].symmetric_difference(names_sets[-1])
        return False, f"variables differ: {sorted(diff)}"
    return True, ""


def _per_variable_match(fps: list[dict]) -> tuple[bool, str]:
    names = list(fps[0]["data_vars"].keys())
    for name in names:
        dtypes = {fp["data_vars"][name]["dtype"] for fp in fps}
        if len(dtypes) > 1:
            return False, f"variable {name} has inconsistent dtype: {sorted(dtypes)}"
        dims_set = {tuple(fp["data_vars"][name]["dims"]) for fp in fps}
        if len(dims_set) > 1:
            return False, f"variable {name} has inconsistent dims: {sorted(str(d) for d in dims_set)}"
        codecs_set = {tuple(fp["data_vars"][name]["codecs"]) for fp in fps}
        if len(codecs_set) > 1:
            return False, f"variable {name} has inconsistent codecs: {sorted(str(c) for c in codecs_set)}"
    return True, ""


def _detect_concat_dim(fps: list[dict]) -> tuple[CubabilityVerdict, str, str | None]:
    all_dims = set().union(*(fp["dims"].keys() for fp in fps))
    # Dims whose size varies are primary candidates for the concat dim.
    # Dims whose coord hash varies (but size is constant) are secondary: they
    # are only considered if no size-varying dim exists.
    size_varying: list[str] = []
    hash_only_varying: list[str] = []
    for d in all_dims:
        sizes = {fp["dims"].get(d) for fp in fps}
        coord_hashes: set[Any] = set()
        for fp in fps:
            c = fp["coords"].get(d)
            coord_hashes.add(c["values_hash"] if c else None)
        if len(sizes) > 1:
            size_varying.append(d)
        elif len(coord_hashes) > 1:
            hash_only_varying.append(d)

    # Prefer size-varying dims as concat candidates.
    if size_varying:
        candidates = size_varying
    else:
        candidates = hash_only_varying

    if len(candidates) == 0:
        return CubabilityVerdict.INCONCLUSIVE, "all granules identical; cannot identify concat dim", None
    if len(candidates) > 1:
        return CubabilityVerdict.INCONCLUSIVE, f"ambiguous concat dim: {sorted(candidates)}", None
    return CubabilityVerdict.FEASIBLE, "", candidates[0]


def _non_concat_dim_sizes_match(fps: list[dict], concat_dim: str) -> tuple[bool, str]:
    all_dims = set().union(*(fp["dims"].keys() for fp in fps)) - {concat_dim}
    for d in sorted(all_dims):
        sizes = {fp["dims"].get(d) for fp in fps}
        if len(sizes) > 1:
            return False, f"non-concat dim {d} size varies: {sorted(s for s in sizes if s is not None)}"
    return True, ""


def _non_concat_coords_match(fps: list[dict], concat_dim: str) -> tuple[bool, str]:
    coord_names = set().union(*(fp["coords"].keys() for fp in fps))
    for name in sorted(coord_names):
        samples = [fp["coords"].get(name) for fp in fps]
        if any(s is None for s in samples):
            continue  # missing on some granules; tolerate
        if concat_dim in samples[0].get("dims", []):  # type: ignore[union-attr]
            continue
        hashes = {s["values_hash"] for s in samples}  # type: ignore[index]
        if len(hashes) > 1:
            return False, f"non-concat coord {name} differs across granules"
    return True, ""


def _chunks_compatible(fps: list[dict], concat_dim: str) -> tuple[bool, str]:
    for name in fps[0]["data_vars"]:
        dims = fps[0]["data_vars"][name]["dims"]
        chunks_lists = [fp["data_vars"][name]["chunks"] for fp in fps]
        if any(c is None for c in chunks_lists):
            continue
        for i, d in enumerate(dims):
            if d == concat_dim:
                continue
            vals = {c[i] for c in chunks_lists if i < len(c)}  # type: ignore[index]
            if len(vals) > 1:
                return False, f"variable {name} chunk shape incompatible on dim {d}: {sorted(vals)}"
    return True, ""


def _coord_endpoint(val: Any, dtype: Any) -> Any:
    """Convert a coord endpoint to a JSON-native, orderable form based on dtype."""
    try:
        if np.issubdtype(dtype, np.datetime64):
            return np.datetime_as_string(val, unit="s")
        if np.issubdtype(dtype, np.integer):
            return int(val)
        if np.issubdtype(dtype, np.floating):
            return float(val)
    except (TypeError, ValueError):
        pass
    return str(val)


def _concat_coord_monotonic(fps: list[dict], concat_dim: str) -> tuple[bool, str]:
    """Check that the concat-dim coord ranges are monotonically non-overlapping.

    min/max are stored as native JSON values (ISO datetime strings, ints, floats,
    or strings) so Python's built-in comparison operators work directly.
    """
    ranges = []
    for fp in fps:
        c = fp["coords"].get(concat_dim)
        if c is None or c.get("min") is None or c.get("max") is None:
            return True, ""  # no coord data; skip
        ranges.append((c["min"], c["max"]))

    try:
        ranges.sort(key=lambda r: r[0])
        for (mn1, mx1), (mn2, mx2) in zip(ranges, ranges[1:]):
            if mx1 > mn2:
                return False, (
                    f"concat coord {concat_dim} overlaps or reverses between "
                    f"{mn1}..{mx1} and {mn2}..{mx2}"
                )
    except TypeError:
        # Mixed incompatible types; should not occur if extraction is consistent.
        return True, ""
    return True, ""


def check_cubability(fingerprints: list[dict]) -> CubabilityResult:
    """Determine whether a collection's granules could form a coherent virtual store.

    Accepts a list of per-granule fingerprint dicts (from extract_fingerprint).
    Returns a CubabilityResult with verdict, optional reason, and optional concat_dim.
    """
    fps = [fp for fp in fingerprints if fp]
    if len(fps) < 2:
        return CubabilityResult(CubabilityVerdict.INCONCLUSIVE, reason="fewer than 2 fingerprints")

    ok, reason = _variables_match(fps)
    if not ok:
        return CubabilityResult(CubabilityVerdict.INCOMPATIBLE, reason)

    ok, reason = _per_variable_match(fps)
    if not ok:
        return CubabilityResult(CubabilityVerdict.INCOMPATIBLE, reason)

    verdict, reason, concat_dim = _detect_concat_dim(fps)
    if concat_dim is None:
        return CubabilityResult(verdict, reason)

    ok, reason = _non_concat_dim_sizes_match(fps, concat_dim)
    if not ok:
        return CubabilityResult(CubabilityVerdict.INCOMPATIBLE, reason, concat_dim)

    ok, reason = _non_concat_coords_match(fps, concat_dim)
    if not ok:
        return CubabilityResult(CubabilityVerdict.INCOMPATIBLE, reason, concat_dim)

    ok, reason = _chunks_compatible(fps, concat_dim)
    if not ok:
        return CubabilityResult(CubabilityVerdict.INCOMPATIBLE, reason, concat_dim)

    ok, reason = _concat_coord_monotonic(fps, concat_dim)
    if not ok:
        return CubabilityResult(CubabilityVerdict.INCOMPATIBLE, reason, concat_dim)

    return CubabilityResult(CubabilityVerdict.FEASIBLE, concat_dim=concat_dim)
