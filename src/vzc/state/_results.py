"""Read-side helpers over ``output/results/*.parquet`` (pyarrow-only).

Replaces DuckDB ``read_parquet`` queries used elsewhere. The Parquet log is
small enough (one row per attempted granule) that it fits comfortably in
memory; the helpers here load all shards once and expose typed rows.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def shard_paths(results_dir: Path | str) -> list[Path]:
    """Sorted list of every Parquet shard under ``results_dir`` (recursive)."""
    p = Path(results_dir)
    if not p.exists():
        return []
    return sorted(p.rglob("*.parquet"))


def load_table(
    results_dir: Path | str,
    *,
    columns: Iterable[str] | None = None,
) -> pa.Table | None:
    """Read every Parquet shard into one :class:`pyarrow.Table`.

    ``columns`` projects only the requested columns when set.
    Returns ``None`` if no shards exist (so callers can short-circuit).
    """
    shards = shard_paths(results_dir)
    if not shards:
        return None
    cols = list(columns) if columns is not None else None
    tables: list[pa.Table] = []
    for shard in shards:
        try:
            tables.append(pq.read_table(shard, columns=cols))
        except Exception:
            # Tolerate per-shard read errors so a corrupted file doesn't block
            # the whole report. Caller can re-run attempt to regenerate.
            continue
    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="default")


def iter_rows(
    results_dir: Path | str,
    *,
    columns: Iterable[str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield each Parquet row as a Python dict."""
    table = load_table(results_dir, columns=columns)
    if table is None:
        return
    yield from table.to_pylist()


def attempted_pairs(results_dir: Path | str) -> set[tuple[str, str]]:
    """``{(collection, granule), ...}`` already present in the Parquet log."""
    table = load_table(
        results_dir, columns=["collection_concept_id", "granule_concept_id"]
    )
    if table is None:
        return set()
    out: set[tuple[str, str]] = set()
    for cid, gid in zip(
        table["collection_concept_id"].to_pylist(),
        table["granule_concept_id"].to_pylist(),
    ):
        if cid is not None and gid is not None:
            out.add((cid, gid))
    return out


def count_rows(
    results_dir: Path | str,
    *,
    where: dict[str, Any] | None = None,
) -> int:
    """Count rows matching ``column == value`` for each entry in ``where``.

    ``where`` is a tiny equality filter: any row that equals all listed
    values is counted. Pass ``None`` (default) to count every row.
    """
    cols = list(where.keys()) if where else None
    table = load_table(results_dir, columns=cols)
    if table is None:
        return 0
    if not where:
        return table.num_rows
    n = 0
    for row in table.to_pylist():
        if all(row.get(k) == v for k, v in where.items()):
            n += 1
    return n
