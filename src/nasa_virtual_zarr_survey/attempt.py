"""Attempt pipeline: Phase 3 (Parsability) and Phase 4 (Datasetability)."""

from __future__ import annotations

import signal
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from virtual_tiff import VirtualTIFF
from virtualizarr.parsers.dmrpp import DMRPPParser
from virtualizarr.parsers.fits import FITSParser
from virtualizarr.parsers.hdf import HDFParser
from virtualizarr.parsers.netcdf3 import NetCDF3Parser
from virtualizarr.parsers.zarr import ZarrParser

from nasa_virtual_zarr_survey.auth import AuthUnavailable, StoreCache
from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.formats import FormatFamily
from nasa_virtual_zarr_survey.types import PendingGranule


@dataclass
class AttemptResult:
    """One row of the append-only per-attempt Parquet log.

    Records independent outcomes for Phase 3 (Parsability) and Phase 4
    (Datasetability) of a single granule attempt, plus identifying fields
    (collection, granule, DAAC, format family) and, on success, the fingerprint
    used by the Cubability phase.

    `dataset_success` is `None` when Phase 4 was not attempted because parsing
    failed. `success` is `True` only when both phases succeeded.
    """

    collection_concept_id: str | None = None
    granule_concept_id: str | None = None
    daac: str | None = None
    format_family: str | None = None
    parser: str | None = None
    stratified: bool | None = None
    attempted_at: datetime | None = None

    # Phase 3: Parsability
    parse_success: bool = False
    parse_error_type: str | None = None
    parse_error_message: str | None = None
    parse_error_traceback: str | None = None
    parse_duration_s: float = 0.0

    # Phase 4: Datasetability (None = not attempted because parse failed)
    dataset_success: bool | None = None
    dataset_error_type: str | None = None
    dataset_error_message: str | None = None
    dataset_error_traceback: str | None = None
    dataset_duration_s: float = 0.0

    # Overall
    success: bool = False  # parse_success AND (dataset_success == True)
    timed_out: bool = False
    timed_out_phase: str | None = None  # "parse" or "dataset"
    duration_s: float = 0.0
    fingerprint: str | None = None


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
    if family is FormatFamily.GEOTIFF:
        return VirtualTIFF()
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
    """Try to open one granule through both phases. Always returns a result; never raises."""
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
        result.parse_error_type = "NoParserAvailable"
        result.parse_error_message = (
            f"No VirtualiZarr parser registered for {family.value}"
        )
        return result

    result.parser = type(parser).__name__
    registry = _build_registry(store, url)

    # Shared state between worker thread and main; only the worker writes phase_ref.
    phase_ref = ["parse"]
    manifest_ref: list = []
    dataset_ref: list = []

    def _call() -> None:
        t_parse = time.monotonic()
        ms = parser(url=url, registry=registry)
        result.parse_duration_s = time.monotonic() - t_parse
        manifest_ref.append(ms)
        result.parse_success = True

        phase_ref[0] = "dataset"
        t_ds = time.monotonic()
        ds = ms.to_virtual_dataset()
        result.dataset_duration_s = time.monotonic() - t_ds
        dataset_ref.append(ds)
        result.dataset_success = True

    t0 = time.monotonic()
    timed_out_phase: str | None = None
    timed_out_flag = False
    exc_info: tuple | None = None

    ex = ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(_call)
        try:
            fut.result(timeout=timeout_s)
        except FuturesTimeoutError:
            # Capture phase NOW, before shutdown lets the thread advance further.
            timed_out_flag = True
            timed_out_phase = phase_ref[0]
        except Exception as e:
            import sys

            exc_info = (type(e), e, sys.exc_info()[2])
    finally:
        # Shut down without waiting so a sleeping thread doesn't block us here.
        ex.shutdown(wait=False, cancel_futures=True)
        result.duration_s = time.monotonic() - t0

    if timed_out_flag:
        result.timed_out = True
        result.timed_out_phase = timed_out_phase
        if timed_out_phase == "parse":
            result.parse_error_type = "TimeoutError"
            result.parse_error_message = f"parse timed out after {timeout_s}s"
        else:
            # Parse succeeded inside worker but dataset construction hung
            result.dataset_success = False
            result.dataset_error_type = "TimeoutError"
            result.dataset_error_message = f"dataset timed out after {timeout_s}s"
    elif exc_info is not None:
        exc = exc_info[1]
        tb_str = _truncate("".join(traceback.format_exception(*exc_info)), 4096)
        if manifest_ref:
            # Parse completed; dataset construction raised
            result.dataset_success = False
            result.dataset_error_type = type(exc).__name__
            result.dataset_error_message = _truncate(str(exc), 2048)
            result.dataset_error_traceback = tb_str
        else:
            # Parser raised
            result.parse_error_type = type(exc).__name__
            result.parse_error_message = _truncate(str(exc), 2048)
            result.parse_error_traceback = tb_str
    else:
        # Both phases completed without raising
        result.success = bool(result.parse_success and result.dataset_success)
        if dataset_ref:
            try:
                from nasa_virtual_zarr_survey.cubability import (
                    extract_fingerprint,
                    fingerprint_to_json,
                )

                result.fingerprint = fingerprint_to_json(
                    extract_fingerprint(dataset_ref[0])
                )
            except Exception:
                # Fingerprint extraction is best-effort; never fail the attempt.
                pass

    return result


_SCHEMA_FIELDS: dict[str, pa.DataType] = {
    "collection_concept_id": pa.string(),
    "granule_concept_id": pa.string(),
    "daac": pa.string(),
    "format_family": pa.string(),
    "parser": pa.string(),
    "stratified": pa.bool_(),
    "attempted_at": pa.timestamp("us", tz="UTC"),
    "parse_success": pa.bool_(),
    "parse_error_type": pa.string(),
    "parse_error_message": pa.string(),
    "parse_error_traceback": pa.string(),
    "parse_duration_s": pa.float64(),
    "dataset_success": pa.bool_(),  # nullable
    "dataset_error_type": pa.string(),
    "dataset_error_message": pa.string(),
    "dataset_error_traceback": pa.string(),
    "dataset_duration_s": pa.float64(),
    "success": pa.bool_(),
    "timed_out": pa.bool_(),
    "timed_out_phase": pa.string(),
    "duration_s": pa.float64(),
    "fingerprint": pa.string(),
}
_SCHEMA = pa.schema(_SCHEMA_FIELDS)


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
        """Buffer a result. Flushes a new shard once the buffer hits `shard_size`."""
        daac = r.daac or "UNKNOWN"
        self._buffers.setdefault(daac, []).append(r)
        if len(self._buffers[daac]) >= self.shard_size:
            self._flush(daac)

    def _flush(self, daac: str) -> None:
        buf = self._buffers.get(daac)
        if not buf:
            return
        cols: dict[str, list] = {field.name: [] for field in _SCHEMA}
        for r in buf:
            cols["collection_concept_id"].append(r.collection_concept_id)
            cols["granule_concept_id"].append(r.granule_concept_id)
            cols["daac"].append(r.daac)
            cols["format_family"].append(r.format_family)
            cols["parser"].append(r.parser)
            cols["stratified"].append(r.stratified)
            cols["attempted_at"].append(r.attempted_at)
            cols["parse_success"].append(r.parse_success)
            cols["parse_error_type"].append(r.parse_error_type)
            cols["parse_error_message"].append(r.parse_error_message)
            cols["parse_error_traceback"].append(r.parse_error_traceback)
            cols["parse_duration_s"].append(r.parse_duration_s)
            cols["dataset_success"].append(r.dataset_success)
            cols["dataset_error_type"].append(r.dataset_error_type)
            cols["dataset_error_message"].append(r.dataset_error_message)
            cols["dataset_error_traceback"].append(r.dataset_error_traceback)
            cols["dataset_duration_s"].append(r.dataset_duration_s)
            cols["success"].append(r.success)
            cols["timed_out"].append(r.timed_out)
            cols["timed_out_phase"].append(r.timed_out_phase)
            cols["duration_s"].append(r.duration_s)
            cols["fingerprint"].append(r.fingerprint)
        pq.write_table(pa.table(cols, schema=_SCHEMA), self._shard_path(daac))
        self._shard_index[daac] = self._shard_index.get(daac, 0) + 1
        self._buffers[daac] = []

    def close(self) -> None:
        """Flush every DAAC's remaining buffered rows to a final shard."""
        for daac in list(self._buffers.keys()):
            self._flush(daac)


def _pending_granules(
    con, results_dir: Path, only_daac: str | None
) -> list[PendingGranule]:
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
        PendingGranule(
            collection_concept_id=r[0],
            granule_concept_id=r[1],
            data_url=r[2],
            daac=r[3],
            provider=r[4],
            format_family=r[5],
            stratified=r[6],
        )
        for r in rows
    ]


def run_attempt(
    db_path: Path | str,
    results_dir: Path | str,
    *,
    timeout_s: int = 60,
    shard_size: int = 500,
    only_daac: str | None = None,
    access: Literal["direct", "external"] = "direct",
) -> int:
    """Attempt every pending granule. Returns count attempted in this call."""
    con = connect(db_path)
    init_schema(con)
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    pending = _pending_granules(con, results_dir, only_daac)
    writer = ResultWriter(results_dir, shard_size=shard_size)
    cache = StoreCache(access=access)

    def _sigint(_sig, _frm):
        writer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sigint)

    from nasa_virtual_zarr_survey.taxonomy import Bucket, classify

    consecutive_forbidden = 0
    FORBIDDEN_ABORT_THRESHOLD = 5

    # Per-collection progress: precompute how many distinct collections we'll touch
    collection_order: list[str] = []
    collection_size: dict[str, int] = {}
    for row in pending:
        cid = row["collection_concept_id"]
        if cid not in collection_size:
            collection_order.append(cid)
            collection_size[cid] = 0
        collection_size[cid] += 1
    total_collections = len(collection_order)

    current_collection: str | None = None
    collection_idx = 0
    collection_pass = 0
    collection_fail = 0

    def _flush_collection_progress() -> None:
        if current_collection is None:
            return
        total = collection_pass + collection_fail
        print(
            f"[{collection_idx}/{total_collections}] {current_collection}: "
            f"{collection_pass}/{total} passed",
            file=sys.stderr,
            flush=True,
        )

    n = 0
    for i, row in enumerate(pending, 1):
        cid = row["collection_concept_id"]
        if cid != current_collection:
            _flush_collection_progress()
            current_collection = cid
            collection_idx += 1
            collection_pass = 0
            collection_fail = 0

        family_str = row["format_family"]
        family = FormatFamily(family_str) if family_str else None
        if family is None or not row["data_url"] or not row["provider"]:
            result = AttemptResult(
                collection_concept_id=row["collection_concept_id"],
                granule_concept_id=row["granule_concept_id"],
                daac=row["daac"],
                format_family=family_str,
                stratified=row["stratified"],
                parse_error_type="SampleInvalid",
                parse_error_message="missing format family, data URL, or provider",
                attempted_at=datetime.now(timezone.utc),
            )
        else:
            try:
                store = cache.get_store(provider=row["provider"], url=row["data_url"])
            except AuthUnavailable as e:
                result = AttemptResult(
                    collection_concept_id=row["collection_concept_id"],
                    granule_concept_id=row["granule_concept_id"],
                    daac=row["daac"],
                    format_family=family_str,
                    stratified=row["stratified"],
                    parse_error_type="AuthUnavailable",
                    parse_error_message=str(e),
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
        if result.success:
            collection_pass += 1
        else:
            collection_fail += 1

        if access == "direct" and not result.success:
            parse_bucket = classify(result.parse_error_type, result.parse_error_message)
            dataset_bucket = classify(
                result.dataset_error_type, result.dataset_error_message
            )
            if parse_bucket is Bucket.FORBIDDEN or dataset_bucket is Bucket.FORBIDDEN:
                consecutive_forbidden += 1
            else:
                consecutive_forbidden = 0
        else:
            consecutive_forbidden = 0

        if consecutive_forbidden >= FORBIDDEN_ABORT_THRESHOLD:
            writer.close()
            raise SystemExit(
                f"\nERROR: {consecutive_forbidden} consecutive direct-S3 requests returned 403/Forbidden.\n"
                "This usually means you are running outside AWS us-west-2. NASA S3\n"
                "buckets only permit direct access from in-region compute.\n\n"
                "Try re-running with: --access external\n"
                f"(First delete output/survey.duckdb and output/results/ since sampled\n"
                "URLs differ between access modes.)\n"
            )

    _flush_collection_progress()
    writer.close()
    return n
