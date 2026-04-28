"""Store construction + on-disk granule cache.

Two halves of the same module:

**Cache layout + I/O** (used by ``prefetch`` and ``attempt --access external``):

* :func:`cache_layout_path` — stable on-disk path for a URL.
* :class:`CacheSizeTracker` — soft cap accounting (consulted by prefetch).
* :func:`download_url_to_cache` — streams bytes through a live store into
  the cache. Atomic via ``.tmp`` rename.
* :class:`ReadOnlyCacheStore` — implements ``ReadableStore``, raises
  ``FileNotFoundError`` on miss. No fall-through to origin.

**Store construction + auth** (used by ``attempt`` and ``prefetch``):

* :func:`make_https_store` — live ``HTTPStore`` for an EDL-authed URL
  (used by prefetch and investigate scripts).
* :class:`DAACStoreCache` — caches per-provider S3 credentials (50-min TTL)
  and builds per-bucket ``S3Store`` objects.
* :class:`StoreCache` — read-side dispatcher: ``access="direct"`` returns a
  live ``S3Store``; ``access="external"`` returns a ``ReadOnlyCacheStore``
  pointing at the cache (fail-fast on miss).
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Protocol, cast
from urllib.parse import urlparse

import earthaccess
from obstore.store import LocalStore

from obspec_utils.protocols import ReadableStore

if TYPE_CHECKING:
    from collections.abc import Buffer

    from obspec import GetOptions, GetResult, GetResultAsync, ObjectMeta
    from obstore.store import HTTPStore, S3Store


_DEFAULT_STREAM_CHUNK = 8 * 1024 * 1024  # 8 MiB
_LOGGER = logging.getLogger(__name__)
_CAP_WARNING_EMITTED = False


class AuthUnavailable(Exception):
    """Raised when a store cannot be built for the requested access mode."""


# ---------------------------------------------------------------------------
# Cache layout + size accounting (shared between read side and write side)
# ---------------------------------------------------------------------------


def cache_layout_path(cache_dir: Path, url: str) -> Path:
    """Return the on-disk path the cache uses for *url*.

    Layout: ``<cache_dir>/<scheme>/<host>/<sha256(scheme://host/path)>``.
    Stable; called from both the writer (prefetch) and the reader
    (:class:`ReadOnlyCacheStore`).
    """
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"url must be scheme://host/..., got {url!r}")
    full_url = f"{parsed.scheme}://{parsed.netloc}/{parsed.path.lstrip('/')}"
    digest = hashlib.sha256(full_url.encode("utf-8")).hexdigest()
    return cache_dir / parsed.scheme / parsed.netloc / digest


def cache_size(cache_dir: Path) -> int:
    """Total size in bytes of all regular files under *cache_dir*.

    Skips ``*.tmp`` partial-write artifacts and any non-file entries.
    """
    if not cache_dir.exists():
        return 0
    total = 0
    for entry in cache_dir.rglob("*"):
        if entry.is_file() and not entry.name.endswith(".tmp"):
            total += entry.stat().st_size
    return total


@dataclass
class CacheSizeTracker:
    """Thread-safe accounting of bytes written to a cache directory.

    One tracker per logical cache. ``prefetch`` holds the tracker and consults
    ``would_exceed`` before each download.
    """

    cache_dir: Path
    max_bytes: int
    _current_size: int = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def __post_init__(self) -> None:
        self._current_size = cache_size(self.cache_dir)

    @property
    def current_size(self) -> int:
        with self._lock:
            return self._current_size

    def would_exceed(self, additional_bytes: int) -> bool:
        with self._lock:
            return self._current_size + additional_bytes > self.max_bytes

    def add(self, n: int) -> None:
        with self._lock:
            self._current_size += n


# ---------------------------------------------------------------------------
# Read side: ReadOnlyCacheStore (used by attempt --access external)
# ---------------------------------------------------------------------------


class ReadOnlyCacheStore(ReadableStore):
    """Read bytes from the cache; raise ``FileNotFoundError`` on miss.

    Implements the obspec ``ReadableStore`` protocol so VirtualiZarr parsers
    can use it transparently. Construct one per ``(scheme, host)`` prefix
    (matching the ``ObjectStoreRegistry`` convention) — the prefix is needed
    to round-trip ``store.get(path)`` calls back to the URL the cache layout
    keys on.

    Strictly read-only: there is no fetch path, no fall-through to origin.
    If a path isn't cached, the parser sees ``FileNotFoundError``. ``attempt
    --access external`` translates that into a clear "run prefetch first"
    error.
    """

    def __init__(self, *, prefix: str, cache_dir: Path) -> None:
        parsed = urlparse(prefix.rstrip("/"))
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"prefix must be scheme://host, got {prefix!r}")
        self._prefix = f"{parsed.scheme}://{parsed.netloc}"
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._local = LocalStore(prefix=str(self._cache_dir.resolve()))

    def _local_path(self, path: str) -> Path:
        full_url = f"{self._prefix}/{path.lstrip('/')}"
        return cache_layout_path(self._cache_dir, full_url)

    def _local_rel(self, path: str) -> str:
        return str(self._local_path(path).relative_to(self._cache_dir))

    def _require_cached(self, path: str) -> None:
        if not self._local_path(path).exists():
            raise FileNotFoundError(
                f"granule not in cache: {self._prefix}/{path.lstrip('/')} — "
                "run `vzc prefetch` first."
            )

    def is_cached(self, path: str) -> bool:
        return self._local_path(path).exists()

    def head(self, path: str) -> "ObjectMeta":
        self._require_cached(path)
        return self._local.head(self._local_rel(path))

    async def head_async(self, path: str) -> "ObjectMeta":
        self._require_cached(path)
        return await self._local.head_async(self._local_rel(path))

    def get(self, path: str, *, options: "GetOptions | None" = None) -> "GetResult":
        self._require_cached(path)
        return self._local.get(self._local_rel(path), options=options)

    async def get_async(
        self, path: str, *, options: "GetOptions | None" = None
    ) -> "GetResultAsync":
        self._require_cached(path)
        return await self._local.get_async(self._local_rel(path), options=options)

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> "Buffer":
        self._require_cached(path)
        return self._local.get_range(
            self._local_rel(path), start=start, end=end, length=length
        )

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> "Buffer":
        self._require_cached(path)
        return await self._local.get_range_async(
            self._local_rel(path), start=start, end=end, length=length
        )

    def get_ranges(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> "Sequence[Buffer]":
        self._require_cached(path)
        return self._local.get_ranges(
            self._local_rel(path), starts=starts, ends=ends, lengths=lengths
        )

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> "Sequence[Buffer]":
        self._require_cached(path)
        return await self._local.get_ranges_async(
            self._local_rel(path), starts=starts, ends=ends, lengths=lengths
        )


# ---------------------------------------------------------------------------
# Write side: download_url_to_cache (used by prefetch)
# ---------------------------------------------------------------------------


class _Streamable(Protocol):
    """Subset of obstore's ``GetResult`` that exposes chunked iteration."""

    def stream(self, min_chunk_size: int = ...) -> Iterator[Any]: ...


class _StreamableAsync(Protocol):
    """Async counterpart of :class:`_Streamable`."""

    def stream(self, min_chunk_size: int = ...) -> AsyncIterator[Any]: ...


def download_url_to_cache(
    *,
    store: Any,
    url: str,
    cache_dir: Path,
    tracker: CacheSizeTracker | None = None,
    on_chunk: Callable[[int], object] | None = None,
) -> int | None:
    """Stream *url* through *store* into ``cache_layout_path(cache_dir, url)``.

    Returns the number of bytes written, or ``None`` when the cache write
    was refused (cap blew, or a write error fell through). On success the
    local file is in place atomically and the tracker is updated.

    Pre-flight HEAD is best-effort: some HTTPS endpoints reject HEAD or omit
    Content-Length; we fall through to GET in that case rather than failing
    the whole download.
    """
    parsed = urlparse(url)
    path = parsed.path.lstrip("/")

    if tracker is not None:
        try:
            meta = store.head(path)
            size = int(meta["size"])
            if tracker.would_exceed(size):
                _warn_cap_exceeded(tracker)
                return None
        except Exception as e:
            _LOGGER.debug("HEAD for %s failed (%s); proceeding to GET", url, e)

    result = cast(_Streamable, store.get(path))
    local = cache_layout_path(cache_dir, url)
    local.parent.mkdir(parents=True, exist_ok=True)
    tmp = local.with_suffix(local.suffix + ".tmp")
    total = 0
    try:
        with tmp.open("wb") as f:
            for chunk in result.stream(min_chunk_size=_DEFAULT_STREAM_CHUNK):
                b = bytes(chunk)
                f.write(b)
                total += len(b)
                if on_chunk is not None:
                    on_chunk(len(b))
        os.replace(tmp, local)
    except OSError:
        tmp.unlink(missing_ok=True)
        return None
    if tracker is not None:
        tracker.add(total)
    return total


def _warn_cap_exceeded(tracker: CacheSizeTracker) -> None:
    global _CAP_WARNING_EMITTED
    if _CAP_WARNING_EMITTED:
        return
    _CAP_WARNING_EMITTED = True
    cur_gb = tracker.current_size / 1024**3
    max_gb = tracker.max_bytes / 1024**3
    _LOGGER.warning(
        "cache size %.1f GB exceeds cap %.1f GB; further granules will not be cached. "
        "clear the cache with `rm -rf %s` or pass --cache-max-size to raise the cap.",
        cur_gb,
        max_gb,
        tracker.cache_dir,
    )


# ---------------------------------------------------------------------------
# S3 store + DAAC credential cache (direct mode)
# ---------------------------------------------------------------------------


class _Creds(NamedTuple):
    creds: dict[str, str]
    minted_at: datetime


@dataclass
class DAACStoreCache:
    """Caches EDL-minted S3 credentials per CMR provider and builds per-bucket
    obstore S3Stores lazily.

    Credentials are cached per provider with a TTL (defaults to 50 minutes,
    below earthaccess's 1-hour expiry). A distinct S3Store is constructed for
    each distinct bucket the caller asks for, sharing the underlying
    credentials.
    """

    ttl: timedelta = timedelta(minutes=50)
    _logged_in: bool = False
    _creds: dict[str, _Creds] = field(default_factory=dict)
    _stores: dict[tuple[str, str], "S3Store"] = field(default_factory=dict)

    def _login(self) -> None:
        if not self._logged_in:
            earthaccess.login(strategy="netrc")
            self._logged_in = True

    def _get_creds(self, provider: str) -> dict[str, str]:
        now = datetime.now(timezone.utc)
        entry = self._creds.get(provider)
        if entry and now - entry.minted_at < self.ttl:
            return entry.creds
        self._login()
        fresh = earthaccess.get_s3_credentials(provider=provider)
        if not fresh or "accessKeyId" not in fresh:
            raise AuthUnavailable(
                f"earthaccess returned no S3 credentials for provider {provider!r}"
            )
        self._creds[provider] = _Creds(creds=fresh, minted_at=now)
        # Invalidate any cached stores for this provider since they hold stale creds.
        self._stores = {k: v for k, v in self._stores.items() if k[0] != provider}
        return fresh

    def get_store(self, *, provider: str, bucket: str) -> "S3Store":
        """Return an obstore S3Store for ``(provider, bucket)``, building it on demand."""
        creds = self._get_creds(provider)
        key = (provider, bucket)
        store = self._stores.get(key)
        if store is not None:
            return store
        store = _build_s3_store(creds, bucket)
        self._stores[key] = store
        return store


def _build_s3_store(creds: dict[str, str], bucket: str) -> "S3Store":
    """Construct an obstore ``S3Store`` for *bucket* using *creds*."""
    from obstore.store import S3Store

    return S3Store(
        bucket=bucket,
        access_key_id=creds["accessKeyId"],
        secret_access_key=creds["secretAccessKey"],
        session_token=creds["sessionToken"],
        region="us-west-2",
    )


# ---------------------------------------------------------------------------
# Live HTTPS store (used by prefetch's downloader; not by StoreCache)
# ---------------------------------------------------------------------------


def make_https_store(url: str, *, token: str | None = None) -> "HTTPStore":
    """Build a live obstore ``HTTPStore`` for ``scheme://host`` of *url*.

    If *token* is None, performs ``earthaccess.login(strategy="netrc")`` to
    mint one. Used by prefetch (write side) and by investigate-generated
    scripts that want a live origin fetch.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise AuthUnavailable(f"make_https_store expects http(s):// URLs, got {url!r}.")
    if token is None:
        earthaccess.login(strategy="netrc")
        token_dict = getattr(earthaccess.__auth__, "token", None) or {}
        token = token_dict.get("access_token") if isinstance(token_dict, dict) else None
        if not token:
            raise AuthUnavailable(
                "earthaccess.login() did not produce a bearer token; check ~/.netrc"
            )

    from obstore.store import HTTPStore

    base = f"{parsed.scheme}://{parsed.netloc}"
    return HTTPStore.from_url(
        base,
        client_options={
            "default_headers": {"Authorization": f"Bearer {token}"},
        },
    )


# ---------------------------------------------------------------------------
# StoreCache: dispatches between direct (S3) and external (read-only cache)
# ---------------------------------------------------------------------------


@dataclass
class StoreCache:
    """Read-side store dispatcher for the survey.

    ``access="direct"`` returns a live ``S3Store`` (in-region, no cache).
    ``access="external"`` requires ``cache_dir`` and returns a
    :class:`ReadOnlyCacheStore` — fail-fast on miss; no live HTTPS fallback.
    """

    access: Literal["direct", "external"] = "direct"
    cache_dir: Path | None = None
    _s3: DAACStoreCache = field(default_factory=DAACStoreCache)
    _ro: dict[str, ReadOnlyCacheStore] = field(default_factory=dict)

    def get_store(self, *, provider: str, url: str) -> "S3Store | ReadableStore":
        """Return a store capable of reading *url* for the given CMR *provider*."""
        parsed = urlparse(url)
        if self.access == "direct":
            if parsed.scheme != "s3":
                raise AuthUnavailable(
                    f"--access direct expects s3:// URLs, got {url!r}. "
                    "The granules table likely has stale URLs from a previous "
                    "--access external sample. Re-run sample with --access direct."
                )
            bucket = parsed.netloc
            if not bucket:
                raise AuthUnavailable(f"cannot extract S3 bucket from url {url!r}")
            return self._s3.get_store(provider=provider, bucket=bucket)

        if parsed.scheme not in ("http", "https"):
            raise AuthUnavailable(
                f"--access external expects http(s):// URLs, got {url!r}. "
                "The granules table likely has stale URLs from a previous "
                "--access direct sample. Re-run sample with --access external."
            )
        if self.cache_dir is None:
            raise AuthUnavailable(
                "--access external is cache-only. Run prefetch first and pass "
                "--cache-dir / NASA_VZ_SURVEY_CACHE_DIR."
            )
        prefix = f"{parsed.scheme}://{parsed.netloc}"
        ro = self._ro.get(prefix)
        if ro is None:
            ro = ReadOnlyCacheStore(prefix=prefix, cache_dir=self.cache_dir)
            self._ro[prefix] = ro
        return ro
