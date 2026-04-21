"""Phase 3 core: dispatch a parser and call open_virtual_dataset with a timeout."""
from __future__ import annotations

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from virtualizarr import open_virtual_dataset
from virtualizarr.parsers.dmrpp import DMRPPParser
from virtualizarr.parsers.fits import FITSParser
from virtualizarr.parsers.hdf import HDFParser
from virtualizarr.parsers.netcdf3 import NetCDF3Parser
from virtualizarr.parsers.zarr import ZarrParser

from nasa_virtual_zarr_survey.formats import FormatFamily


@dataclass
class AttemptResult:
    collection_concept_id: str | None = None
    granule_concept_id: str | None = None
    daac: str | None = None
    format_family: str | None = None
    parser: str | None = None
    success: bool = False
    error_type: str | None = None
    error_message: str | None = None
    error_traceback: str | None = None
    duration_s: float = 0.0
    timed_out: bool = False
    attempted_at: datetime | None = None
    stratified: bool | None = None


def dispatch_parser(family: FormatFamily) -> Any | None:
    """Return a freshly-instantiated VirtualiZarr parser, or None if unsupported."""
    if family in (FormatFamily.NETCDF4, FormatFamily.HDF5):
        return HDFParser()
    if family is FormatFamily.NETCDF3:
        return NetCDF3Parser()
    if family is FormatFamily.DMRPP:
        return DMRPPParser()
    if family is FormatFamily.FITS:
        return FITSParser()
    if family is FormatFamily.ZARR:
        return ZarrParser()
    return None


def _build_registry(store: Any, url: str) -> Any:
    """Build an ObjectStoreRegistry pointing the URL's scheme+bucket at `store`."""
    from urllib.parse import urlparse

    from obspec_utils.registry import ObjectStoreRegistry

    parsed = urlparse(url)
    scheme = parsed.scheme or "s3"
    bucket = parsed.netloc
    return ObjectStoreRegistry({f"{scheme}://{bucket}": store})


def _truncate(s: str, limit: int) -> str:
    return s if len(s) <= limit else s[:limit] + "...[truncated]"


def attempt_one(
    *,
    url: str,
    family: FormatFamily,
    store: Any,
    timeout_s: int = 60,
    collection_concept_id: str | None = None,
    granule_concept_id: str | None = None,
    daac: str | None = None,
    stratified: bool | None = None,
) -> AttemptResult:
    """Try to open one granule. Always returns a result; never raises."""
    result = AttemptResult(
        collection_concept_id=collection_concept_id,
        granule_concept_id=granule_concept_id,
        daac=daac,
        format_family=family.value,
        attempted_at=datetime.now(timezone.utc),
        stratified=stratified,
    )

    parser = dispatch_parser(family)
    if parser is None:
        result.error_type = "NoParserAvailable"
        result.error_message = f"No VirtualiZarr parser registered for {family.value}"
        return result

    result.parser = type(parser).__name__
    registry = _build_registry(store, url)

    def _call() -> None:
        open_virtual_dataset(url=url, registry=registry, parser=parser)

    t0 = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_call)
            fut.result(timeout=timeout_s)
        result.success = True
    except FuturesTimeoutError:
        result.timed_out = True
        result.error_type = "TimeoutError"
        result.error_message = f"timed out after {timeout_s}s"
    except Exception as e:
        result.error_type = type(e).__name__
        result.error_message = _truncate(str(e), 2048)
        result.error_traceback = _truncate(traceback.format_exc(), 4096)
    finally:
        result.duration_s = time.monotonic() - t0

    return result


import signal
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nasa_virtual_zarr_survey.auth import AuthUnavailable, DAACStoreCache
from nasa_virtual_zarr_survey.db import connect, init_schema


_SCHEMA = pa.schema([
    ("collection_concept_id", pa.string()),
    ("granule_concept_id", pa.string()),
    ("daac", pa.string()),
    ("format_family", pa.string()),
    ("parser", pa.string()),
    ("success", pa.bool_()),
    ("error_type", pa.string()),
    ("error_message", pa.string()),
    ("error_traceback", pa.string()),
    ("duration_s", pa.float64()),
    ("timed_out", pa.bool_()),
    ("attempted_at", pa.timestamp("us", tz="UTC")),
    ("stratified", pa.bool_()),
])


class ResultWriter:
    """Append-only, DAAC-partitioned Parquet writer. Rotates shards every `shard_size` rows."""

    def __init__(self, base_dir: Path, shard_size: int = 500):
        self.base_dir = Path(base_dir)
        self.shard_size = shard_size
        self._buffers: dict[str, list[AttemptResult]] = {}
        self._shard_index: dict[str, int] = {}

    def _shard_path(self, daac: str) -> Path:
        idx = self._shard_index.get(daac, 0)
        d = self.base_dir / f"DAAC={daac}"
        d.mkdir(parents=True, exist_ok=True)
        while (d / f"part-{idx:04d}.parquet").exists():
            idx += 1
        self._shard_index[daac] = idx
        return d / f"part-{idx:04d}.parquet"

    def append(self, r: AttemptResult) -> None:
        daac = r.daac or "UNKNOWN"
        self._buffers.setdefault(daac, []).append(r)
        if len(self._buffers[daac]) >= self.shard_size:
            self._flush(daac)

    def _flush(self, daac: str) -> None:
        buf = self._buffers.get(daac)
        if not buf:
            return
        cols = {field.name: [] for field in _SCHEMA}
        for r in buf:
            cols["collection_concept_id"].append(r.collection_concept_id)
            cols["granule_concept_id"].append(r.granule_concept_id)
            cols["daac"].append(r.daac)
            cols["format_family"].append(r.format_family)
            cols["parser"].append(r.parser)
            cols["success"].append(r.success)
            cols["error_type"].append(r.error_type)
            cols["error_message"].append(r.error_message)
            cols["error_traceback"].append(r.error_traceback)
            cols["duration_s"].append(r.duration_s)
            cols["timed_out"].append(r.timed_out)
            cols["attempted_at"].append(r.attempted_at)
            cols["stratified"].append(r.stratified)
        pq.write_table(pa.table(cols, schema=_SCHEMA), self._shard_path(daac))
        self._shard_index[daac] = self._shard_index.get(daac, 0) + 1
        self._buffers[daac] = []

    def close(self) -> None:
        for daac in list(self._buffers.keys()):
            self._flush(daac)


def _pending_granules(con, results_dir: Path, only_daac: str | None) -> list[dict]:
    """Return granule rows for which no Parquet row exists yet."""
    results_glob = str(results_dir / "**" / "*.parquet")
    q = """
        SELECT c.concept_id AS collection_concept_id,
               g.granule_concept_id,
               g.data_url,
               c.daac,
               c.provider,
               c.format_family,
               g.stratified
        FROM granules g
        JOIN collections c ON c.concept_id = g.collection_concept_id
        WHERE c.skip_reason IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM read_parquet(?, union_by_name=true, hive_partitioning=true) r
            WHERE r.collection_concept_id = g.collection_concept_id
              AND r.granule_concept_id   = g.granule_concept_id
          )
    """
    params: list = [results_glob]
    if only_daac:
        q += " AND c.daac = ?"
        params.append(only_daac)
    q += " ORDER BY c.daac, c.concept_id, g.temporal_bin"

    try:
        rows = con.execute(q, params).fetchall()
    except Exception:
        # No Parquet files yet -- read_parquet fails; fall back to "all granules".
        fallback = """
            SELECT c.concept_id, g.granule_concept_id, g.data_url, c.daac, c.provider,
                   c.format_family, g.stratified
            FROM granules g JOIN collections c ON c.concept_id = g.collection_concept_id
            WHERE c.skip_reason IS NULL
        """
        params2: list = []
        if only_daac:
            fallback += " AND c.daac = ?"
            params2.append(only_daac)
        fallback += " ORDER BY c.daac, c.concept_id, g.temporal_bin"
        rows = con.execute(fallback, params2).fetchall()

    return [
        {"collection_concept_id": r[0], "granule_concept_id": r[1],
         "data_url": r[2], "daac": r[3], "provider": r[4],
         "format_family": r[5], "stratified": r[6]}
        for r in rows
    ]


def run_attempt(
    db_path: Path | str,
    results_dir: Path | str,
    *,
    timeout_s: int = 60,
    shard_size: int = 500,
    only_daac: str | None = None,
) -> int:
    """Attempt every pending granule. Returns count attempted in this call."""
    con = connect(db_path)
    init_schema(con)
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    pending = _pending_granules(con, results_dir, only_daac)
    writer = ResultWriter(results_dir, shard_size=shard_size)
    cache = DAACStoreCache()

    def _sigint(_sig, _frm):
        writer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sigint)

    n = 0
    for i, row in enumerate(pending, 1):
        family_str = row["format_family"]
        family = FormatFamily(family_str) if family_str else None
        if family is None or not row["data_url"]:
            result = AttemptResult(
                collection_concept_id=row["collection_concept_id"],
                granule_concept_id=row["granule_concept_id"],
                daac=row["daac"], format_family=family_str,
                stratified=row["stratified"],
                error_type="SampleInvalid",
                error_message="missing format family or data URL",
                attempted_at=datetime.now(timezone.utc),
            )
        else:
            try:
                store = cache.get_store(row["provider"])
            except AuthUnavailable as e:
                result = AttemptResult(
                    collection_concept_id=row["collection_concept_id"],
                    granule_concept_id=row["granule_concept_id"],
                    daac=row["daac"], format_family=family_str,
                    stratified=row["stratified"],
                    error_type="AuthUnavailable",
                    error_message=str(e),
                    attempted_at=datetime.now(timezone.utc),
                )
            else:
                result = attempt_one(
                    url=row["data_url"],
                    family=family,
                    store=store,
                    timeout_s=timeout_s,
                    collection_concept_id=row["collection_concept_id"],
                    granule_concept_id=row["granule_concept_id"],
                    daac=row["daac"],
                    stratified=row["stratified"],
                )
        writer.append(result)
        n += 1
        if i % 500 == 0:
            print(f"[heartbeat] attempted {i}/{len(pending)}", file=sys.stderr)

    writer.close()
    return n
