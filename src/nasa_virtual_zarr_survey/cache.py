"""Persistent on-disk granule cache for the survey.

Wraps any ``obspec.ReadableStore`` so that fetched objects are written to a
configurable cache directory and subsequent reads (including range reads) are
served from disk. Whole-granule granularity, append-only, soft total size cap.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from obstore.store import LocalStore

from obspec_utils.protocols import ReadableStore

if TYPE_CHECKING:
    from collections.abc import Buffer

    from obspec import GetOptions, GetResult, GetResultAsync, ObjectMeta


_LOGGER = logging.getLogger(__name__)
_CAP_WARNING_EMITTED = False


def cache_size(cache_dir: Path) -> int:
    """Return the total size in bytes of all regular files under *cache_dir*.

    Missing directory returns 0. Walks once; skips ``*.tmp`` partial-write
    artifacts and any non-file entries.
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
    """Shared, thread-safe accounting of bytes written to a cache directory.

    One tracker per logical cache (i.e., per ``cache_dir``). Multiple
    ``DiskCachingReadableStore`` instances within the same process should share
    a tracker so a single cap applies across all wrapped stores.
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


class DiskCachingReadableStore(ReadableStore):
    """Wraps a ``ReadableStore`` so fetched objects persist to disk on first read.

    Subsequent reads — including range reads — are served from disk without a
    network round trip. The wrapper is transparent: it implements the same
    obspec read protocol as the underlying store and is constructed once per
    ``(scheme, host)`` (or ``(provider, bucket)``) by ``StoreCache``.

    Parameters
    ----------
    store
        The underlying ``ReadableStore`` (typically an ``S3Store`` or
        ``HTTPStore``).
    prefix
        ``scheme://host`` for paths passed to ``store``. Used to compute a
        stable, host-namespaced cache key. The wrapper is not portable across
        different prefixes — use one wrapper per prefix.
    tracker
        Shared ``CacheSizeTracker`` enforcing the cap across all wrappers
        backed by the same cache dir.
    """

    def __init__(
        self,
        store: ReadableStore,
        *,
        prefix: str,
        tracker: CacheSizeTracker,
    ) -> None:
        self._store = store
        self._prefix = prefix.rstrip("/")
        self._tracker = tracker
        self._cache_dir = tracker.cache_dir
        parsed = urlparse(self._prefix)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"prefix must be scheme://host, got {prefix!r}")
        self._scheme = parsed.scheme
        self._host = parsed.netloc
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._local = LocalStore(prefix=str(self._cache_dir.resolve()))

    def _local_path(self, path: str) -> Path:
        full_url = f"{self._prefix}/{path.lstrip('/')}"
        digest = hashlib.sha256(full_url.encode("utf-8")).hexdigest()
        return self._cache_dir / self._scheme / self._host / digest

    def _local_rel(self, path: str) -> str:
        local = self._local_path(path)
        return str(local.relative_to(self._cache_dir))

    def head(self, path: str) -> "ObjectMeta":
        return self._store.head(path)

    async def head_async(self, path: str) -> "ObjectMeta":
        return await self._store.head_async(path)

    def get(self, path: str, *, options: "GetOptions | None" = None) -> "GetResult":
        if self._local_path(path).exists():
            return self._local.get(self._local_rel(path), options=options)
        return self._fetch_and_cache(path, options=options)

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> "Buffer":
        if self._local_path(path).exists():
            return self._local.get_range(
                self._local_rel(path), start=start, end=end, length=length
            )
        self._fetch_and_cache(path)
        return self._local.get_range(
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
        if self._local_path(path).exists():
            return self._local.get_ranges(
                self._local_rel(path), starts=starts, ends=ends, lengths=lengths
            )
        self._fetch_and_cache(path)
        return self._local.get_ranges(
            self._local_rel(path), starts=starts, ends=ends, lengths=lengths
        )

    def _fetch_and_cache(
        self, path: str, *, options: "GetOptions | None" = None
    ) -> "GetResult":
        # Pre-flight: head to learn size for cap check.
        meta = self._store.head(path)
        size = int(meta["size"])
        if self._tracker.would_exceed(size):
            self._warn_cap_exceeded()
            return self._store.get(path, options=options)

        # Fetch full object.
        result = self._store.get(path)
        data = bytes(result.buffer())

        # Write atomically: tmp + replace.
        local = self._local_path(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        tmp = local.with_suffix(local.suffix + ".tmp")
        try:
            tmp.write_bytes(data)
            os.replace(tmp, local)
        except OSError:
            # Best-effort cleanup of tmp; fall through to direct fetch.
            tmp.unlink(missing_ok=True)
            return self._store.get(path, options=options)

        self._tracker.add(len(data))
        return self._local.get(self._local_rel(path), options=options)

    def _warn_cap_exceeded(self) -> None:
        global _CAP_WARNING_EMITTED
        if _CAP_WARNING_EMITTED:
            return
        _CAP_WARNING_EMITTED = True
        cur_gb = self._tracker.current_size / 1024**3
        max_gb = self._tracker.max_bytes / 1024**3
        _LOGGER.warning(
            "cache size %.1f GB exceeds cap %.1f GB; further granules will not be cached. "
            "clear the cache with `rm -rf %s` or pass --cache-max-size to raise the cap.",
            cur_gb,
            max_gb,
            self._cache_dir,
        )

    async def get_async(
        self, path: str, *, options: "GetOptions | None" = None
    ) -> "GetResultAsync":
        if self._local_path(path).exists():
            return await self._local.get_async(self._local_rel(path), options=options)
        return await self._fetch_and_cache_async(path, options=options)

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> "Buffer":
        if not self._local_path(path).exists():
            await self._fetch_and_cache_async(path)
        return await self._local.get_range_async(
            self._local_rel(path), start=start, end=end, length=length
        )

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> "Sequence[Buffer]":
        if not self._local_path(path).exists():
            await self._fetch_and_cache_async(path)
        return await self._local.get_ranges_async(
            self._local_rel(path), starts=starts, ends=ends, lengths=lengths
        )

    async def _fetch_and_cache_async(
        self, path: str, *, options: "GetOptions | None" = None
    ) -> "GetResultAsync":
        meta = await self._store.head_async(path)
        size = int(meta["size"])
        if self._tracker.would_exceed(size):
            self._warn_cap_exceeded()
            return await self._store.get_async(path, options=options)

        result = await self._store.get_async(path)
        data = bytes(await result.buffer_async())

        local = self._local_path(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        tmp = local.with_suffix(local.suffix + ".tmp")
        try:
            tmp.write_bytes(data)
            os.replace(tmp, local)
        except OSError:
            tmp.unlink(missing_ok=True)
            return await self._store.get_async(path, options=options)

        self._tracker.add(len(data))
        return await self._local.get_async(self._local_rel(path), options=options)
