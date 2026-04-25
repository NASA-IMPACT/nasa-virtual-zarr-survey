"""End-to-end integration test for the local granule cache.

Validates the full plumbing using a ``MemoryStore`` backend so no network
is involved. Two ``DiskCachingReadableStore`` instances backed by the same
cache directory cooperate: bytes written by the first are read back by the
second without issuing any further ``get()`` on the underlying store.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("obstore")


def test_second_run_serves_from_cache(tmp_path: Path, monkeypatch) -> None:
    """Two DiskCachingReadableStore instances backed by the same cache dir
    cooperate: data written by the first is read by the second without
    issuing any further get() on the underlying store."""
    from obstore.store import MemoryStore

    from nasa_virtual_zarr_survey.cache import (
        CacheSizeTracker,
        DiskCachingReadableStore,
    )

    underlying = MemoryStore()
    underlying.put("file.nc", b"GRANULE_BYTES")

    tracker_a = CacheSizeTracker(tmp_path, max_bytes=10_000)
    cached_a = DiskCachingReadableStore(underlying, prefix="s3://b", tracker=tracker_a)
    out1 = bytes(cached_a.get("file.nc").buffer())
    assert out1 == b"GRANULE_BYTES"

    # Simulate a fresh process: new tracker walks dir on init.
    tracker_b = CacheSizeTracker(tmp_path, max_bytes=10_000)
    assert tracker_b.current_size == len(b"GRANULE_BYTES")

    # Wrap a fresh underlying that does NOT have the data — proves the second
    # read is served from disk only.
    empty_underlying = MemoryStore()
    cached_b = DiskCachingReadableStore(
        empty_underlying, prefix="s3://b", tracker=tracker_b
    )
    out2 = bytes(cached_b.get("file.nc").buffer())
    assert out2 == b"GRANULE_BYTES"
