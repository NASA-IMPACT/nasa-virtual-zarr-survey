"""Unit tests for cache layout, size tracking, ReadOnlyCacheStore, and the
prefetch download helper."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from vzc.pipeline._stores import (
    CacheSizeTracker,
    ReadOnlyCacheStore,
    cache_layout_path,
    cache_size,
    download_url_to_cache,
)


# --- cache_size helper ----------------------------------------------------


def test_cache_size_empty_dir(tmp_path: Path) -> None:
    assert cache_size(tmp_path) == 0


def test_cache_size_missing_dir_returns_zero(tmp_path: Path) -> None:
    assert cache_size(tmp_path / "nope") == 0


def test_cache_size_sums_files_recursively(tmp_path: Path) -> None:
    (tmp_path / "a.bin").write_bytes(b"x" * 100)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.bin").write_bytes(b"x" * 250)
    # .tmp files (partial writes) are ignored.
    (tmp_path / "c.bin.tmp").write_bytes(b"x" * 9999)
    assert cache_size(tmp_path) == 350


# --- CacheSizeTracker -----------------------------------------------------


def test_tracker_initial_size_walks_dir(tmp_path: Path) -> None:
    (tmp_path / "x.bin").write_bytes(b"a" * 500)
    tracker = CacheSizeTracker(tmp_path, max_bytes=1000)
    assert tracker.current_size == 500


def test_tracker_would_exceed(tmp_path: Path) -> None:
    tracker = CacheSizeTracker(tmp_path, max_bytes=1000)
    assert not tracker.would_exceed(500)
    assert not tracker.would_exceed(1000)
    assert tracker.would_exceed(1001)


def test_tracker_add_increments(tmp_path: Path) -> None:
    tracker = CacheSizeTracker(tmp_path, max_bytes=1000)
    tracker.add(200)
    assert tracker.current_size == 200
    tracker.add(300)
    assert tracker.current_size == 500


# --- cache_layout_path ----------------------------------------------------


def test_cache_layout_path_uses_sha256(tmp_path: Path) -> None:
    url = "s3://bucket/key/in/bucket.nc"
    p = cache_layout_path(tmp_path, url)
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert p == tmp_path / "s3" / "bucket" / digest


def test_cache_layout_path_https(tmp_path: Path) -> None:
    url = "https://data.example.gov/path/file.nc"
    p = cache_layout_path(tmp_path, url)
    assert p.parent == tmp_path / "https" / "data.example.gov"


def test_cache_layout_path_rejects_bare_paths(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        cache_layout_path(tmp_path, "/no/scheme.nc")


# --- ReadOnlyCacheStore ---------------------------------------------------


def _seed_cache_file(cache_dir: Path, url: str, contents: bytes) -> Path:
    """Write `contents` to the path the cache layout would use for `url`."""
    p = cache_layout_path(cache_dir, url)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(contents)
    return p


def test_read_only_store_is_cached_check(tmp_path: Path) -> None:
    store = ReadOnlyCacheStore(prefix="https://x.example", cache_dir=tmp_path)
    assert not store.is_cached("foo.nc")
    _seed_cache_file(tmp_path, "https://x.example/foo.nc", b"abc")
    assert store.is_cached("foo.nc")


def test_read_only_store_get_returns_cached_bytes(tmp_path: Path) -> None:
    _seed_cache_file(tmp_path, "https://x.example/foo.nc", b"hello world")
    store = ReadOnlyCacheStore(prefix="https://x.example", cache_dir=tmp_path)
    result = store.get("foo.nc")
    assert bytes(result.bytes()) == b"hello world"


def test_read_only_store_get_raises_on_miss(tmp_path: Path) -> None:
    store = ReadOnlyCacheStore(prefix="https://x.example", cache_dir=tmp_path)
    with pytest.raises(FileNotFoundError, match="prefetch"):
        store.get("missing.nc")


def test_read_only_store_get_range(tmp_path: Path) -> None:
    _seed_cache_file(tmp_path, "https://x.example/foo.nc", b"abcdefghij")
    store = ReadOnlyCacheStore(prefix="https://x.example", cache_dir=tmp_path)
    result = store.get_range("foo.nc", start=2, end=6)
    assert bytes(result) == b"cdef"


def test_read_only_store_invalid_prefix_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="prefix"):
        ReadOnlyCacheStore(prefix="not-a-url", cache_dir=tmp_path)


# --- download_url_to_cache ------------------------------------------------


class _FakeStream:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def stream(self, min_chunk_size: int = 0):
        yield from self._chunks


def test_download_writes_atomically_and_updates_tracker(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    tracker = CacheSizeTracker(cache_dir, max_bytes=1_000_000)

    url = "https://x.example/data/foo.nc"
    fake_store = MagicMock()
    fake_store.head.return_value = {"size": 7}
    fake_store.get.return_value = _FakeStream([b"abc", b"defg"])

    written = download_url_to_cache(
        store=fake_store,
        url=url,
        cache_dir=cache_dir,
        tracker=tracker,
    )

    assert written == 7
    cached = cache_layout_path(cache_dir, url)
    assert cached.read_bytes() == b"abcdefg"
    assert tracker.current_size == 7


def test_download_refuses_when_cap_would_blow(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    tracker = CacheSizeTracker(cache_dir, max_bytes=10)

    url = "https://x.example/big.nc"
    fake_store = MagicMock()
    fake_store.head.return_value = {"size": 999}
    fake_store.get.return_value = _FakeStream([b"x" * 999])

    written = download_url_to_cache(
        store=fake_store,
        url=url,
        cache_dir=cache_dir,
        tracker=tracker,
    )
    assert written is None
    # Nothing written to disk.
    assert not cache_layout_path(cache_dir, url).exists()


def test_download_tolerates_head_failure(tmp_path: Path) -> None:
    """HTTPS endpoints that reject HEAD or omit Content-Length should still
    succeed via GET."""
    cache_dir = tmp_path / "cache"
    tracker = CacheSizeTracker(cache_dir, max_bytes=1_000_000)

    url = "https://x.example/no-head.nc"
    fake_store = MagicMock()
    fake_store.head.side_effect = RuntimeError("HEAD not supported")
    fake_store.get.return_value = _FakeStream([b"payload"])

    written = download_url_to_cache(
        store=fake_store,
        url=url,
        cache_dir=cache_dir,
        tracker=tracker,
    )
    assert written == 7
