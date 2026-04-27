"""Attempt pipeline: Phase 3 (Parsability), Phase 4a (Datasetability), Phase 4b (Datatreeability)."""

from __future__ import annotations

import signal
import sys
import threading
import time
import traceback
from collections.abc import Mapping
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
from nasa_virtual_zarr_survey.db_session import SurveySession
from nasa_virtual_zarr_survey.formats import FormatFamily
from nasa_virtual_zarr_survey.overrides import (
    CollectionOverride,
    OverrideRegistry,
    apply_to_dataset_call,
    apply_to_datatree_call,
)
from nasa_virtual_zarr_survey.types import PendingGranule

DEFAULT_OVERRIDES_PATH = Path("config/collection_overrides.toml")


@dataclass
class AttemptResult:
    """One row of the append-only per-attempt Parquet log.

    Records independent outcomes for Phase 3 (Parsability), Phase 4a
    (Datasetability), and Phase 4b (Datatreeability) of a single granule
    attempt, plus identifying fields (collection, granule, DAAC, format family)
    and, on success, the fingerprint used by the Cubability phase.

    `dataset_success` and `datatree_success` are `None` when the respective
    phase was not attempted because parsing failed.  `success` is `True` when
    parse succeeded AND at least one of dataset or datatree succeeded.
    """

    collection_concept_id: str | None = None
    granule_concept_id: str | None = None
    daac: str | None = None
    format_family: str | None = None
    parser: str | None = None
    attempted_at: datetime | None = None

    # Phase 3: Parsability
    parse_success: bool = False
    parse_error_type: str | None = None
    parse_error_message: str | None = None
    parse_error_traceback: str | None = None
    parse_duration_s: float = 0.0

    # Phase 4a: Datasetability (None = not attempted because parse failed)
    dataset_success: bool | None = None
    dataset_error_type: str | None = None
    dataset_error_message: str | None = None
    dataset_error_traceback: str | None = None
    dataset_duration_s: float = 0.0

    # Phase 4b: Datatreeability (None = not attempted because parse failed)
    datatree_success: bool | None = None
    datatree_error_type: str | None = None
    datatree_error_message: str | None = None
    datatree_error_traceback: str | None = None
    datatree_duration_s: float = 0.0

    # Overall
    success: bool = False  # parse_success AND (dataset_success OR datatree_success)
    override_applied: bool = False
    timed_out: bool = False
    timed_out_phase: str | None = None  # "parse", "dataset", or "datatree"
    duration_s: float = 0.0
    fingerprint: str | None = None


def dispatch_parser(
    family: FormatFamily,
    kwargs: Mapping[str, Any] | None = None,
) -> Any | None:
    """Return a freshly-instantiated VirtualiZarr parser, or None if unsupported."""
    kw = dict(kwargs or {})
    if family in (FormatFamily.NETCDF4, FormatFamily.HDF5):
        return HDFParser(**kw)
    if family is FormatFamily.NETCDF3:
        return NetCDF3Parser(**kw)
    if family is FormatFamily.DMRPP:
        return DMRPPParser(**kw)
    if family is FormatFamily.FITS:
        return FITSParser(**kw)
    if family is FormatFamily.ZARR:
        return ZarrParser(**kw)
    if family is FormatFamily.GEOTIFF:
        return VirtualTIFF(**kw)
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
    override: CollectionOverride | None = None,
) -> AttemptResult:
    """Try to open one granule through both phases. Always returns a result; never raises."""
    ov = override or CollectionOverride()
    result = AttemptResult(
        collection_concept_id=collection_concept_id,
        granule_concept_id=granule_concept_id,
        daac=daac,
        format_family=family.value,
        attempted_at=datetime.now(timezone.utc),
        override_applied=not ov.is_empty(),
    )

    parser = dispatch_parser(family, kwargs=ov.parser_kwargs)
    if parser is None:
        result.parse_error_type = "NoParserAvailable"
        result.parse_error_message = (
            f"No VirtualiZarr parser registered for {family.value}"
        )
        return result

    result.parser = type(parser).__name__
    registry = _build_registry(store, url)

    # Shared state written by the worker thread only.
    manifest_ref: list = []
    dataset_ref: list = []
    datatree_ref: list = []

    # Three events, one per phase; set by the worker as each phase completes
    # (whether by success, exception, or early-exit).
    parse_done = threading.Event()
    dataset_done = threading.Event()
    datatree_done = threading.Event()

    def _runner() -> None:
        # Phase 3: Parsability
        try:
            t = time.monotonic()
            ms = parser(url=url, registry=registry)
            result.parse_duration_s = time.monotonic() - t
            manifest_ref.append(ms)
            result.parse_success = True
        except BaseException as exc:
            tb_str = _truncate(traceback.format_exc(), 4096)
            result.parse_error_type = type(exc).__name__
            result.parse_error_message = _truncate(str(exc), 2048)
            result.parse_error_traceback = tb_str
        finally:
            parse_done.set()

        if not result.parse_success:
            dataset_done.set()
            datatree_done.set()
            return

        # Phase 4a: Datasetability
        if ov.skip_dataset:
            result.dataset_success = None  # signals "skipped via override"
            dataset_done.set()
        else:
            try:
                t = time.monotonic()
                ds = apply_to_dataset_call(manifest_ref[0], ov.dataset_kwargs)
                result.dataset_duration_s = time.monotonic() - t
                dataset_ref.append(ds)
                result.dataset_success = True
            except BaseException as exc:
                tb_str = _truncate(traceback.format_exc(), 4096)
                result.dataset_success = False
                result.dataset_error_type = type(exc).__name__
                result.dataset_error_message = _truncate(str(exc), 2048)
                result.dataset_error_traceback = tb_str
            finally:
                dataset_done.set()

        # Phase 4b: Datatreeability
        if ov.skip_datatree:
            result.datatree_success = None
            datatree_done.set()
        else:
            try:
                t = time.monotonic()
                dt = apply_to_datatree_call(manifest_ref[0], ov.datatree_kwargs)
                result.datatree_duration_s = time.monotonic() - t
                datatree_ref.append(dt)
                result.datatree_success = True
            except BaseException as exc:
                tb_str = _truncate(traceback.format_exc(), 4096)
                result.datatree_success = False
                result.datatree_error_type = type(exc).__name__
                result.datatree_error_message = _truncate(str(exc), 2048)
                result.datatree_error_traceback = tb_str
            finally:
                datatree_done.set()

    # Daemon thread: if the worker hangs on a blocking I/O call we still want
    # the interpreter to exit cleanly once the outer process is done.
    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()

    t0 = time.monotonic()
    deadline = t0 + timeout_s
    # timeout_s is the total budget across all phases, not a per-phase cap.
    # Each event.wait gets the remaining budget so a single granule can't
    # consume up to 3*timeout_s of wall time.
    for phase_name, event in [
        ("parse", parse_done),
        ("dataset", dataset_done),
        ("datatree", datatree_done),
    ]:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not event.wait(timeout=remaining):
            elapsed = time.monotonic() - t0
            result.timed_out = True
            result.timed_out_phase = phase_name
            msg = (
                f"{phase_name} did not complete within {timeout_s}s "
                f"overall budget (elapsed {elapsed:.1f}s)"
            )
            if phase_name == "parse":
                result.parse_error_type = "TimeoutError"
                result.parse_error_message = msg
            elif phase_name == "dataset":
                result.dataset_success = False
                result.dataset_error_type = "TimeoutError"
                result.dataset_error_message = msg
            else:
                result.datatree_success = False
                result.datatree_error_type = "TimeoutError"
                result.datatree_error_message = msg
            break

    result.duration_s = time.monotonic() - t0

    # Compute overall success: parse AND at least one of dataset/datatree succeeded.
    if result.parse_success and (
        result.dataset_success is True or result.datatree_success is True
    ):
        result.success = True

    # Fingerprint: use xr.Dataset when available; skip when only datatree succeeded.
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
    "datatree_success": pa.bool_(),  # nullable
    "datatree_error_type": pa.string(),
    "datatree_error_message": pa.string(),
    "datatree_error_traceback": pa.string(),
    "datatree_duration_s": pa.float64(),
    "success": pa.bool_(),
    "override_applied": pa.bool_(),
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
            cols["datatree_success"].append(r.datatree_success)
            cols["datatree_error_type"].append(r.datatree_error_type)
            cols["datatree_error_message"].append(r.datatree_error_message)
            cols["datatree_error_traceback"].append(r.datatree_error_traceback)
            cols["datatree_duration_s"].append(r.datatree_duration_s)
            cols["success"].append(r.success)
            cols["override_applied"].append(r.override_applied)
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
    con,
    results_dir: Path,
    only_daac: str | None,
    only_collection: str | None = None,
    max_granule_bytes: int | None = None,
) -> list[PendingGranule]:
    """Return granule rows for which no Parquet row exists yet.

    When ``max_granule_bytes`` is set, collections with any sampled granule
    whose ``size_bytes`` exceeds it are excluded entirely. Granules with
    unknown size (NULL) pass through.
    """
    results_glob = str(results_dir / "**" / "*.parquet")
    oversize_clause = (
        " AND g.collection_concept_id NOT IN ("
        " SELECT DISTINCT collection_concept_id FROM granules"
        " WHERE size_bytes > ?)"
    )
    q = """
        SELECT c.concept_id AS collection_concept_id,
               g.granule_concept_id,
               g.data_url,
               c.daac,
               c.provider,
               c.format_family
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
    if only_collection:
        q += " AND c.concept_id = ?"
        params.append(only_collection)
    if max_granule_bytes is not None:
        q += oversize_clause
        params.append(max_granule_bytes)
    q += " ORDER BY c.daac, c.concept_id, g.stratification_bin"

    try:
        rows = con.execute(q, params).fetchall()
    except Exception:
        # No Parquet files yet -- read_parquet fails; fall back to "all granules".
        fallback = """
            SELECT c.concept_id, g.granule_concept_id, g.data_url, c.daac, c.provider,
                   c.format_family
            FROM granules g JOIN collections c ON c.concept_id = g.collection_concept_id
            WHERE c.skip_reason IS NULL
        """
        params2: list = []
        if only_daac:
            fallback += " AND c.daac = ?"
            params2.append(only_daac)
        if only_collection:
            fallback += " AND c.concept_id = ?"
            params2.append(only_collection)
        if max_granule_bytes is not None:
            fallback += oversize_clause
            params2.append(max_granule_bytes)
        fallback += " ORDER BY c.daac, c.concept_id, g.stratification_bin"
        rows = con.execute(fallback, params2).fetchall()

    return [
        PendingGranule(
            collection_concept_id=r[0],
            granule_concept_id=r[1],
            data_url=r[2],
            daac=r[3],
            provider=r[4],
            format_family=r[5],
        )
        for r in rows
    ]


def run_attempt(
    session: SurveySession,
    results_dir: Path | str,
    *,
    timeout_s: int = 60,
    shard_size: int = 500,
    only_daac: str | None = None,
    only_collection: str | None = None,
    access: Literal["direct", "external"] = "direct",
    cache_dir: Path | None = None,
    cache_max_bytes: int = 50 * 1024**3,
    overrides_path: Path | str = DEFAULT_OVERRIDES_PATH,
    no_overrides: bool = False,
    skip_override_validation: bool = False,
    max_granule_bytes: int | None = None,
    cache_only: bool = False,
) -> int:
    """Attempt every pending granule. Returns count attempted in this call."""
    con = session.con
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    if no_overrides:
        override_registry = OverrideRegistry.empty()
    else:
        override_registry = OverrideRegistry.from_toml(overrides_path)
    format_for: dict[str, FormatFamily] = {}
    for cid, fam_str in con.execute(
        "SELECT concept_id, format_family FROM collections "
        "WHERE format_family IS NOT NULL"
    ).fetchall():
        try:
            format_for[cid] = FormatFamily(fam_str)
        except ValueError:
            continue
    if not no_overrides and not skip_override_validation:
        override_registry.validate(format_for=format_for)

    pending = _pending_granules(
        con, results_dir, only_daac, only_collection, max_granule_bytes
    )
    if max_granule_bytes is not None:
        # Surface the filter so the operator knows why some collections are absent.
        skipped = con.execute(
            "SELECT COUNT(DISTINCT collection_concept_id) FROM granules "
            "WHERE size_bytes > ?",
            [max_granule_bytes],
        ).fetchone()
        n_skip = (skipped[0] if skipped else 0) or 0
        if n_skip:
            print(
                f"attempt: skipping {n_skip} collection(s) with sampled granules "
                f"> {max_granule_bytes / 1024**3:.1f} GB",
                file=sys.stderr,
                flush=True,
            )

    if cache_only:
        if cache_dir is None:
            raise ValueError("cache_only=True requires cache_dir to be set")
        from nasa_virtual_zarr_survey.cache import cache_layout_path

        before = len(pending)
        kept: list[PendingGranule] = []
        for p in pending:
            url = p["data_url"]
            if url and cache_layout_path(cache_dir, url).exists():
                kept.append(p)
        pending = kept
        print(
            f"attempt: --cache-only kept {len(pending)} of {before} pending "
            f"granule(s) found in {cache_dir}",
            file=sys.stderr,
            flush=True,
        )
    writer = ResultWriter(results_dir, shard_size=shard_size)
    cache = StoreCache(
        access=access,
        cache_dir=cache_dir,
        cache_max_bytes=cache_max_bytes,
    )

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
                    override=override_registry.for_collection(
                        row["collection_concept_id"]
                    ),
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
