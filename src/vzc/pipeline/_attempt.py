"""Attempt pipeline: Phase 3 (Parsability), Phase 4a (Datasetability), Phase 4b (Datatreeability)."""

from __future__ import annotations

import logging
import threading
import time
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from virtual_tiff import VirtualTIFF
from virtualizarr.parsers.dmrpp import DMRPPParser
from virtualizarr.parsers.fits import FITSParser
from virtualizarr.parsers.hdf import HDFParser
from virtualizarr.parsers.netcdf3 import NetCDF3Parser
from virtualizarr.parsers.zarr import ZarrParser

from vzc._config import AccessMode, DEFAULT_OVERRIDES_PATH
from vzc.core.formats import FormatFamily
from vzc.core.types import PendingGranule
from vzc.pipeline._overrides import (
    CollectionOverride,
    OverrideRegistry,
    apply_to_dataset_call,
    apply_to_datatree_call,
)
from vzc.pipeline._stores import AuthUnavailable, StoreCache
from vzc.state._io import SurveyState

_LOG = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class GranuleInfo:
    """Survey-side identifying fields for one granule.

    Used as the input bundle for :class:`SingleGranuleAttempt`. ``provider``
    is needed when the caller wants the attempt to construct its own store
    via a :class:`StoreCache`; pass-through callers that already have a store
    can leave it ``None``.
    """

    url: str
    family: FormatFamily
    collection_concept_id: str | None = None
    granule_concept_id: str | None = None
    daac: str | None = None
    provider: str | None = None


@dataclass(frozen=True)
class SingleGranuleAttempt:
    """One single-granule attempt: the shared core for survey, probe, repro.

    ``run()`` delegates to :func:`attempt_one`. Either ``store`` or ``cache``
    must be set: ``store`` skips store construction (the caller built one),
    ``cache`` uses :class:`StoreCache` to fetch credentials and (optionally)
    cache bytes on disk.

    The dataclass shape exists so the survey, the ``probe`` diagnostic, and
    ``repro`` reproduction scripts can hand the same input bundle to the
    same code path. When ``attempt.py`` changes (e.g., the timeout discipline,
    fingerprint extraction, override application), all three follow.
    """

    granule: "GranuleInfo"
    override: "CollectionOverride | None" = None
    timeout_s: float = 60
    store: Any = None
    cache: "StoreCache | None" = None

    def run(self) -> "AttemptResult":
        store = self.store
        if store is None:
            if self.cache is None:
                raise ValueError(
                    "SingleGranuleAttempt: either `store` or `cache` must be set"
                )
            if not self.granule.provider:
                raise ValueError(
                    "SingleGranuleAttempt: granule.provider is required to "
                    "construct a store via cache"
                )
            store = self.cache.get_store(
                provider=self.granule.provider, url=self.granule.url
            )
        return attempt_one(
            url=self.granule.url,
            family=self.granule.family,
            store=store,
            timeout_s=int(self.timeout_s),
            collection_concept_id=self.granule.collection_concept_id,
            granule_concept_id=self.granule.granule_concept_id,
            daac=self.granule.daac,
            override=self.override,
        )


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
        # The three phase blocks below catch `BaseException` (not `Exception`)
        # so the per-phase event still fires and the result row still gets
        # populated when a parser raises something exotic (e.g. `SystemExit`
        # from a misbehaving third-party library). Do not narrow to
        # `Exception` without replacing this safety net.
        #
        # This does NOT suppress user interrupts: `KeyboardInterrupt` is
        # delivered to the main thread, not this daemon worker, and the
        # `event.wait` loop below remains interruptible.

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
            from vzc.pipeline._cubability import (
                extract_fingerprint,
                fingerprint_to_json,
            )

            result.fingerprint = fingerprint_to_json(
                extract_fingerprint(dataset_ref[0])
            )
        except Exception as exc:
            # Best-effort: don't fail the attempt, but make the failure visible
            # so a regression in extract_fingerprint can't silently disable
            # the cubability phase.
            _LOG.warning(
                "fingerprint extraction failed for %s: %s: %s",
                result.granule_concept_id,
                type(exc).__name__,
                exc,
            )

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


def _pending_attempts(
    state: "SurveyState",
    access: AccessMode,
    results_dir: Path,
) -> list[PendingGranule]:
    """Return granule rows that have no Parquet result yet, projected as PendingGranule.

    Joins :class:`SurveyState` granules with their collection's daac, provider,
    and format_family, filters out collections with a non-empty ``skip_reason``,
    and excludes pairs already present in the Parquet log under ``results_dir``.
    """
    from vzc.state._io import pending_granules

    pending = pending_granules(state, results_dir)
    coll_by_id = state.collections_by_id()
    out: list[PendingGranule] = []
    for g in pending:
        coll = coll_by_id[g.collection_concept_id]
        out.append(
            PendingGranule(
                collection_concept_id=g.collection_concept_id,
                granule_concept_id=g.granule_concept_id,
                data_url=g.url_for(access),
                daac=coll.daac,
                provider=coll.provider,
                format_family=coll.format_family,
            )
        )
    return out


def attempt(
    *,
    access: AccessMode = "direct",
    timeout_s: int = 60,
) -> int:
    """Attempt every pending granule. Returns count attempted in this call.

    Reads ``output/state.json`` and ``output/results/`` (relative to cwd).
    With ``access="external"``, reads granule bytes from the cache at
    ``NASA_VZ_SURVEY_CACHE_DIR`` (default ``~/.cache/nasa-virtual-zarr-survey``)
    and fails fast on miss — run ``prefetch`` first. With ``access="direct"``
    the cache is unused (in-region S3 is fast and free).
    """
    from vzc._config import (
        DEFAULT_RESULTS,
        DEFAULT_STATE_PATH,
        cache_dir as _cache_dir,
    )
    from vzc.state._io import load_state

    state = load_state(DEFAULT_STATE_PATH)
    return _run_attempt(
        state,
        access=access,
        timeout_s=timeout_s,
        results_dir=DEFAULT_RESULTS,
        cache_dir=_cache_dir() if access == "external" else None,
    )


def _run_attempt(
    state: "SurveyState",
    *,
    access: AccessMode,
    timeout_s: int,
    results_dir: Path,
    cache_dir: Path | None,
    shard_size: int = 500,
    overrides_path: Path | str = DEFAULT_OVERRIDES_PATH,
    skip_override_validation: bool = False,
) -> int:
    """Run the attempt loop against an explicit state + paths.

    Used by :func:`attempt` (with state loaded from ``output/state.json``)
    and by :func:`vzc.snapshot.run` (with state loaded from a
    locked-sample JSON, results pinned under ``output/snapshots/<slug>/``).
    """
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    if access == "external" and cache_dir is None:
        raise ValueError("access='external' requires cache_dir; run prefetch first.")

    override_registry = OverrideRegistry.from_toml(overrides_path)
    format_for: dict[str, FormatFamily] = {}
    for c in state.collections:
        if c.format_family is None:
            continue
        try:
            format_for[c.concept_id] = FormatFamily(c.format_family)
        except ValueError:
            continue
    if not skip_override_validation:
        override_registry.validate(format_for=format_for)

    pending = _pending_attempts(state, access, results_dir)

    writer = ResultWriter(results_dir, shard_size=shard_size)
    cache = StoreCache(access=access, cache_dir=cache_dir)

    from vzc.core.taxonomy import Bucket, classify

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
        _LOG.info(
            "[%d/%d] %s: %d/%d passed",
            collection_idx,
            total_collections,
            current_collection,
            collection_pass,
            total,
        )

    def _attempt_row(row: PendingGranule) -> AttemptResult:
        """Build an AttemptResult for one pending granule via SingleGranuleAttempt.

        Returns a synthetic SampleInvalid / AuthUnavailable row for cases that
        can't reach the shared core (no format family or URL, or a credential
        fetch failure).
        """
        family_str = row["format_family"]
        family = FormatFamily(family_str) if family_str else None
        if family is None or not row["data_url"] or not row["provider"]:
            return AttemptResult(
                collection_concept_id=row["collection_concept_id"],
                granule_concept_id=row["granule_concept_id"],
                daac=row["daac"],
                format_family=family_str,
                parse_error_type="SampleInvalid",
                parse_error_message="missing format family, data URL, or provider",
                attempted_at=datetime.now(timezone.utc),
            )
        try:
            return SingleGranuleAttempt(
                granule=GranuleInfo(
                    url=row["data_url"],
                    family=family,
                    collection_concept_id=row["collection_concept_id"],
                    granule_concept_id=row["granule_concept_id"],
                    daac=row["daac"],
                    provider=row["provider"],
                ),
                override=override_registry.for_collection(row["collection_concept_id"]),
                timeout_s=timeout_s,
                cache=cache,
            ).run()
        except AuthUnavailable as e:
            return AttemptResult(
                collection_concept_id=row["collection_concept_id"],
                granule_concept_id=row["granule_concept_id"],
                daac=row["daac"],
                format_family=family_str,
                parse_error_type="AuthUnavailable",
                parse_error_message=str(e),
                attempted_at=datetime.now(timezone.utc),
            )

    n = 0
    try:
        for i, row in enumerate(pending, 1):
            cid = row["collection_concept_id"]
            if cid != current_collection:
                _flush_collection_progress()
                current_collection = cid
                collection_idx += 1
                collection_pass = 0
                collection_fail = 0

            result = _attempt_row(row)
            writer.append(result)
            n += 1
            if result.success:
                collection_pass += 1
                _LOG.info("  %s: pass", row["granule_concept_id"])
            else:
                collection_fail += 1
                err = result.dataset_error_type or result.parse_error_type or "unknown"
                _LOG.info("  %s: fail (%s)", row["granule_concept_id"], err)

            if access == "direct" and not result.success:
                parse_bucket = classify(
                    result.parse_error_type, result.parse_error_message
                )
                dataset_bucket = classify(
                    result.dataset_error_type, result.dataset_error_message
                )
                if (
                    parse_bucket is Bucket.FORBIDDEN
                    or dataset_bucket is Bucket.FORBIDDEN
                ):
                    consecutive_forbidden += 1
                else:
                    consecutive_forbidden = 0
            else:
                consecutive_forbidden = 0

            if consecutive_forbidden >= FORBIDDEN_ABORT_THRESHOLD:
                raise SystemExit(
                    f"\nERROR: {consecutive_forbidden} consecutive direct-S3 requests returned 403/Forbidden.\n"
                    "This usually means you are running outside AWS us-west-2. NASA S3\n"
                    "buckets only permit direct access from in-region compute.\n\n"
                    "Try re-running with: --access external\n"
                    "Sampled URLs are recorded for both modes; no resampling is needed.\n"
                )

        _flush_collection_progress()
    finally:
        writer.close()
    return n
