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
) -> AttemptResult:
    """Try to open one granule. Always returns a result; never raises."""
    result = AttemptResult(
        collection_concept_id=collection_concept_id,
        granule_concept_id=granule_concept_id,
        daac=daac,
        format_family=family.value,
        attempted_at=datetime.now(timezone.utc),
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
