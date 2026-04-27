from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest
from obstore.store import MemoryStore

from nasa_virtual_zarr_survey.cache import (
    CacheSizeTracker,
    DiskCachingReadableStore,
    cache_size,
)


@pytest.fixture(autouse=True)
def _reset_cap_warning(monkeypatch):
    monkeypatch.setattr("nasa_virtual_zarr_survey.cache._CAP_WARNING_EMITTED", False)


# --- Task A1: cache_size helper ---


def test_cache_size_empty_dir(tmp_path: Path) -> None:
    assert cache_size(tmp_path) == 0


def test_cache_size_missing_dir_returns_zero(tmp_path: Path) -> None:
    assert cache_size(tmp_path / "nope") == 0


def test_cache_size_sums_files_recursively(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"x" * 100)
    sub = tmp_path / "scheme" / "host"
    sub.mkdir(parents=True)
    (sub / "abc").write_bytes(b"y" * 250)
    assert cache_size(tmp_path) == 350


# --- Task A2: CacheSizeTracker ---


def test_tracker_initial_size_walks_dir(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"x" * 100)
    tracker = CacheSizeTracker(tmp_path, max_bytes=1000)
    assert tracker.current_size == 100


def test_tracker_would_exceed(tmp_path: Path) -> None:
    tracker = CacheSizeTracker(tmp_path, max_bytes=1000)
    assert not tracker.would_exceed(500)
    assert tracker.would_exceed(1001)


def test_tracker_add_increments(tmp_path: Path) -> None:
    tracker = CacheSizeTracker(tmp_path, max_bytes=1000)
    tracker.add(400)
    assert tracker.current_size == 400
    tracker.add(300)
    assert tracker.current_size == 700


# --- Task A3: DiskCachingReadableStore.__init__ and _local_path ---


def test_local_path_layout(tmp_path: Path) -> None:
    underlying = MemoryStore()
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(
        underlying, prefix="s3://my-bucket", tracker=tracker
    )
    p = cached._local_path("key/in/bucket.nc")
    # cache_dir / scheme / host / sha256(prefix + "/" + path)
    assert p.parent == tmp_path / "s3" / "my-bucket"
    assert len(p.name) == 64  # hex sha256
    # Same input -> same output
    assert cached._local_path("key/in/bucket.nc") == p
    # Different path -> different name
    assert cached._local_path("other.nc").name != p.name


def test_local_path_for_https_prefix(tmp_path: Path) -> None:
    underlying = MemoryStore()
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(
        underlying, prefix="https://daac.example.com", tracker=tracker
    )
    p = cached._local_path("path/to/file.nc")
    assert p.parent == tmp_path / "https" / "daac.example.com"


def test_fetch_and_cache_tolerates_head_failure(tmp_path: Path) -> None:
    """A HEAD that throws (e.g., LAADS without Content-Length) must not abort the GET."""

    class _HeadFailing:
        def __init__(self, inner) -> None:  # type: ignore[no-untyped-def]
            self._inner = inner

        def head(self, path):  # type: ignore[no-untyped-def]
            raise RuntimeError("Content-Length Header missing from response")

        def get(self, path, **kw):  # type: ignore[no-untyped-def]
            return self._inner.get(path, **kw)

        def get_range(self, path, **kw):  # type: ignore[no-untyped-def]
            return self._inner.get_range(path, **kw)

        def get_ranges(self, path, **kw):  # type: ignore[no-untyped-def]
            return self._inner.get_ranges(path, **kw)

        async def head_async(self, path):  # type: ignore[no-untyped-def]
            raise RuntimeError("Content-Length Header missing from response")

        async def get_async(self, path, **kw):  # type: ignore[no-untyped-def]
            return await self._inner.get_async(path, **kw)

        async def get_range_async(self, path, **kw):  # type: ignore[no-untyped-def]
            return await self._inner.get_range_async(path, **kw)

        async def get_ranges_async(self, path, **kw):  # type: ignore[no-untyped-def]
            return await self._inner.get_ranges_async(path, **kw)

    inner = MemoryStore()
    inner.put("foo.bin", b"hello")
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(
        _HeadFailing(inner), prefix="s3://b", tracker=tracker
    )
    result = cached.get("foo.bin")
    assert bytes(result.buffer()) == b"hello"
    # File was cached even though HEAD threw.
    assert cached.is_cached("foo.bin")
    assert tracker.current_size == 5


def test_is_cached_and_cached_path(tmp_path: Path) -> None:
    underlying_inner = MemoryStore()
    underlying_inner.put("foo.bin", b"hello")
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(
        underlying_inner, prefix="s3://b", tracker=tracker
    )
    assert cached.is_cached("foo.bin") is False
    assert cached.cached_path("foo.bin") is None
    cached.get("foo.bin")
    assert cached.is_cached("foo.bin") is True
    p = cached.cached_path("foo.bin")
    assert p is not None and p.read_bytes() == b"hello"


# --- Task A4: head() delegates to underlying store ---


def test_head_delegates_to_underlying(tmp_path: Path) -> None:
    underlying = MemoryStore()
    underlying.put("foo.bin", b"x" * 100)
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)
    meta = cached.head("foo.bin")
    assert meta["size"] == 100


# --- Task A5: Cache hit path helpers and tests ---


class _CountingStore:
    """Thin spy: records every method call before delegating."""

    def __init__(self, inner) -> None:  # type: ignore[no-untyped-def]
        self._inner = inner
        self.calls: list[tuple[str, str]] = []

    def get(self, path, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("get", path))
        return self._inner.get(path, **kwargs)

    def get_range(self, path, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("get_range", path))
        return self._inner.get_range(path, **kwargs)

    def get_ranges(self, path, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("get_ranges", path))
        return self._inner.get_ranges(path, **kwargs)

    def head(self, path):  # type: ignore[no-untyped-def]
        self.calls.append(("head", path))
        return self._inner.head(path)

    def get_async(self, path, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("get_async", path))
        return self._inner.get_async(path, **kwargs)

    def get_range_async(self, path, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("get_range_async", path))
        return self._inner.get_range_async(path, **kwargs)

    def get_ranges_async(self, path, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("get_ranges_async", path))
        return self._inner.get_ranges_async(path, **kwargs)

    def head_async(self, path):  # type: ignore[no-untyped-def]
        self.calls.append(("head_async", path))
        return self._inner.head_async(path)


def _prepopulate(cache: DiskCachingReadableStore, path: str, data: bytes) -> None:
    """Write *data* into the cache layout so the next read is a hit."""
    local = cache._local_path(path)
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(data)
    cache._tracker.add(len(data))


def test_get_cache_hit_serves_from_disk(tmp_path: Path) -> None:
    underlying = _CountingStore(MemoryStore())
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)
    _prepopulate(cached, "foo.bin", b"hello")

    result = cached.get("foo.bin")
    assert bytes(result.buffer()) == b"hello"
    assert ("get", "foo.bin") not in underlying.calls


def test_get_range_cache_hit_serves_from_disk(tmp_path: Path) -> None:
    underlying = _CountingStore(MemoryStore())
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)
    _prepopulate(cached, "foo.bin", b"abcdefghij")

    out = cached.get_range("foo.bin", start=2, end=6)
    assert bytes(out) == b"cdef"
    assert all(call[0] != "get_range" for call in underlying.calls)


def test_get_ranges_cache_hit_serves_from_disk(tmp_path: Path) -> None:
    underlying = _CountingStore(MemoryStore())
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)
    _prepopulate(cached, "foo.bin", b"abcdefghij")

    parts = cached.get_ranges("foo.bin", starts=[0, 5], ends=[3, 8])
    assert [bytes(p) for p in parts] == [b"abc", b"fgh"]
    assert all(call[0] != "get_ranges" for call in underlying.calls)


# --- Task A6: Cache miss path ---


def test_get_cache_miss_fetches_writes_serves(tmp_path: Path) -> None:
    underlying_inner = MemoryStore()
    underlying_inner.put("foo.bin", b"hello world")
    underlying = _CountingStore(underlying_inner)
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)

    result = cached.get("foo.bin")
    assert bytes(result.buffer()) == b"hello world"

    # File written at expected location
    assert cached._local_path("foo.bin").exists()
    # Tracker updated
    assert tracker.current_size == 11

    # Next call is a hit — no further get on underlying
    pre_calls = list(underlying.calls)
    cached.get("foo.bin")
    new_get_calls = [c for c in underlying.calls[len(pre_calls) :] if c[0] == "get"]
    assert new_get_calls == []


def test_get_range_cache_miss_caches_full_then_serves_range(tmp_path: Path) -> None:
    underlying_inner = MemoryStore()
    underlying_inner.put("foo.bin", b"abcdefghij")
    underlying = _CountingStore(underlying_inner)
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)

    out = cached.get_range("foo.bin", start=2, end=6)
    assert bytes(out) == b"cdef"
    # Full file is on disk
    assert cached._local_path("foo.bin").read_bytes() == b"abcdefghij"


# --- Task A7: Cap behavior ---


def test_get_cap_exceeded_falls_through_no_file_written(tmp_path: Path, caplog) -> None:
    underlying_inner = MemoryStore()
    underlying_inner.put("big.bin", b"x" * 500)
    underlying = _CountingStore(underlying_inner)
    tracker = CacheSizeTracker(tmp_path, max_bytes=100)  # too small
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)

    with caplog.at_level(logging.WARNING):
        result = cached.get("big.bin")

    assert bytes(result.buffer()) == b"x" * 500
    # No file written
    assert not cached._local_path("big.bin").exists()
    # Tracker unchanged
    assert tracker.current_size == 0
    # Warning emitted exactly once
    cap_warnings = [r for r in caplog.records if "cache size" in r.getMessage()]
    assert len(cap_warnings) == 1


def test_cap_warning_emits_only_once_per_process(tmp_path: Path, caplog) -> None:
    underlying_inner = MemoryStore()
    underlying_inner.put("a.bin", b"x" * 500)
    underlying_inner.put("b.bin", b"y" * 500)
    underlying = _CountingStore(underlying_inner)
    tracker = CacheSizeTracker(tmp_path, max_bytes=100)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)

    with caplog.at_level(logging.WARNING):
        cached.get("a.bin")
        cached.get("b.bin")

    cap_warnings = [r for r in caplog.records if "cache size" in r.getMessage()]
    assert len(cap_warnings) == 1


# --- Task A8: Async read methods ---


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


def test_get_async_cache_miss_then_hit(tmp_path: Path) -> None:
    underlying_inner = MemoryStore()
    underlying_inner.put("foo.bin", b"hello")
    underlying = _CountingStore(underlying_inner)
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)

    async def first() -> bytes:
        r = await cached.get_async("foo.bin")
        return bytes(await r.buffer_async())

    async def second() -> bytes:
        r = await cached.get_async("foo.bin")
        return bytes(await r.buffer_async())

    assert _run(first()) == b"hello"
    assert cached._local_path("foo.bin").exists()
    assert _run(second()) == b"hello"


# --- Task A9: Stale .tmp files are ignored on read ---


def test_stale_tmp_treated_as_miss(tmp_path: Path) -> None:
    underlying_inner = MemoryStore()
    underlying_inner.put("foo.bin", b"hello")
    underlying = _CountingStore(underlying_inner)
    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker)

    # Simulate a crashed previous write: only tmp exists.
    local = cached._local_path("foo.bin")
    local.parent.mkdir(parents=True, exist_ok=True)
    tmp = local.with_suffix(local.suffix + ".tmp")
    tmp.write_bytes(b"PARTIAL")

    result = cached.get("foo.bin")
    assert bytes(result.buffer()) == b"hello"
    # Real file written, tmp gone (replaced).
    assert local.exists()
    assert not tmp.exists()


# --- Task A10: Underlying-store error during fetch leaves no .tmp behind ---


def test_underlying_error_leaves_no_tmp(tmp_path: Path) -> None:
    class _RaisingStore:
        def head(self, path):  # type: ignore[no-untyped-def]
            return {
                "path": path,
                "size": 5,
                "last_modified": None,
                "e_tag": None,
                "version": None,
            }

        def get(self, path, **kwargs):  # type: ignore[no-untyped-def]
            raise ConnectionError("boom")

    tracker = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached = DiskCachingReadableStore(
        _RaisingStore(),
        prefix="s3://b",
        tracker=tracker,  # type: ignore[arg-type]
    )

    import pytest as _pytest

    with _pytest.raises(ConnectionError):
        cached.get("foo.bin")

    local = cached._local_path("foo.bin")
    assert not local.exists()
    assert not local.with_suffix(local.suffix + ".tmp").exists()
