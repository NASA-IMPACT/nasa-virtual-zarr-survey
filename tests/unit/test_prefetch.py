"""Unit tests for prefetch (Phase 2.5)."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pytest
from obstore.store import MemoryStore

from nasa_virtual_zarr_survey.auth import StoreCache
from nasa_virtual_zarr_survey.cache import DiskCachingReadableStore
from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.prefetch import run_prefetch
from tests.conftest import insert_collection, insert_granule


@pytest.fixture(autouse=True)
def _reset_cap_warning(monkeypatch):
    monkeypatch.setattr("nasa_virtual_zarr_survey.cache._CAP_WARNING_EMITTED", False)


class _FailingStore:
    """MemoryStore wrapper that raises on configured keys."""

    def __init__(self, inner, fail_paths: set[str]) -> None:  # type: ignore[no-untyped-def]
        self._inner = inner
        self._fail = fail_paths

    def get(self, path, **kw):  # type: ignore[no-untyped-def]
        if path in self._fail:
            raise RuntimeError(f"simulated network failure for {path}")
        return self._inner.get(path, **kw)

    def get_range(self, path, **kw):  # type: ignore[no-untyped-def]
        return self._inner.get_range(path, **kw)

    def get_ranges(self, path, **kw):  # type: ignore[no-untyped-def]
        return self._inner.get_ranges(path, **kw)

    def head(self, path, **kw):  # type: ignore[no-untyped-def]
        return self._inner.head(path, **kw)

    def get_async(self, path, **kw):  # type: ignore[no-untyped-def]
        return self._inner.get_async(path, **kw)

    def get_range_async(self, path, **kw):  # type: ignore[no-untyped-def]
        return self._inner.get_range_async(path, **kw)

    def get_ranges_async(self, path, **kw):  # type: ignore[no-untyped-def]
        return self._inner.get_ranges_async(path, **kw)

    def head_async(self, path, **kw):  # type: ignore[no-untyped-def]
        return self._inner.head_async(path, **kw)


def _patch_get_store(
    monkeypatch, payloads: dict[str, dict[str, bytes]], *, fail: set[str] | None = None
):
    """Replace StoreCache.get_store with a MemoryStore-backed wrapper.

    `payloads` maps each ``scheme://host`` prefix to ``{path: bytes}``. ``fail``
    is a set of paths that raise on ``.get(...)`` to simulate a fetch failure.
    """
    fail_set = fail or set()

    def fake_get_store(self, *, provider, url):  # type: ignore[no-untyped-def]
        parsed = urlparse(url)
        prefix = f"{parsed.scheme}://{parsed.netloc}"
        key = (parsed.scheme, parsed.netloc)
        cached = self._wrapped.get(key)
        if cached is not None:
            return cached
        inner = MemoryStore()
        for p, data in payloads.get(prefix, {}).items():
            inner.put(p, data)
        if fail_set:
            inner = _FailingStore(inner, fail_set)
        wrapped = DiskCachingReadableStore(inner, prefix=prefix, tracker=self.tracker)
        self._wrapped[key] = wrapped
        return wrapped

    monkeypatch.setattr(StoreCache, "get_store", fake_get_store)


def _seed(
    con, ranked: list[tuple[str, int, list[tuple[str, str, int | None]]]]
) -> None:
    """Seed the DB with `ranked = [(concept_id, rank, [(granule_id, url, size)])]`."""
    for cid, rank, granules in ranked:
        insert_collection(con, cid, popularity_rank=rank, usage_score=1000 - rank)
        for i, (gid, url, size) in enumerate(granules):
            insert_granule(
                con,
                cid,
                gid,
                data_url=url,
                size_bytes=size,
                stratification_bin=i,
            )


def test_run_prefetch_happy_path(
    tmp_db_path: Path, tmp_path: Path, monkeypatch
) -> None:
    con = connect(tmp_db_path)
    init_schema(con)
    _seed(
        con,
        [
            (
                "C-1",
                1,
                [("G-1a", "s3://b/c1/a.nc", 100), ("G-1b", "s3://b/c1/b.nc", 200)],
            ),
            ("C-2", 2, [("G-2a", "s3://b/c2/a.nc", 150)]),
        ],
    )
    con.close()

    _patch_get_store(
        monkeypatch,
        {
            "s3://b": {
                "c1/a.nc": b"x" * 100,
                "c1/b.nc": b"y" * 200,
                "c2/a.nc": b"z" * 150,
            }
        },
    )

    summary = run_prefetch(
        tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=10_000
    )

    assert summary["collections_considered"] == 2
    assert summary["collections_with_fetches"] == 2
    assert summary["granules_fetched"] == 3
    assert summary["granules_failed"] == 0
    assert summary["bytes_added"] == 450
    assert summary["stopped_at_rank"] == 0  # walked through everything

    # Log rows: one fetch/ok per granule.
    con = connect(tmp_db_path)
    rows = con.execute(
        "SELECT collection_concept_id, action, status FROM prefetch_log "
        "ORDER BY ts, granule_concept_id"
    ).fetchall()
    statuses = [(r[0], r[1], r[2]) for r in rows]
    assert ("C-1", "fetch", "ok") in statuses
    assert ("C-2", "fetch", "ok") in statuses


def test_run_prefetch_stops_at_cap_with_overshoot(
    tmp_db_path: Path, tmp_path: Path, monkeypatch
) -> None:
    """First collection finishes even if it crosses the cap; second is not entered."""
    con = connect(tmp_db_path)
    init_schema(con)
    _seed(
        con,
        [
            # C-1 alone is 1500 bytes; cap is 1000, so it overshoots.
            (
                "C-1",
                1,
                [("G-1a", "s3://b/c1/a.nc", 1000), ("G-1b", "s3://b/c1/b.nc", 500)],
            ),
            ("C-2", 2, [("G-2a", "s3://b/c2/a.nc", 100)]),
        ],
    )
    con.close()

    _patch_get_store(
        monkeypatch,
        {
            "s3://b": {
                "c1/a.nc": b"x" * 1000,
                "c1/b.nc": b"y" * 500,
                "c2/a.nc": b"z" * 100,
            }
        },
    )

    summary = run_prefetch(
        tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=1000
    )

    # C-1 fully cached (overshoot allowed); C-2 never entered.
    assert summary["granules_fetched"] == 2
    assert summary["bytes_added"] == 1500
    assert summary["stopped_at_rank"] == 1

    con = connect(tmp_db_path)
    fetched_collections = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT collection_concept_id FROM prefetch_log "
            "WHERE action = 'fetch' AND status = 'ok'"
        ).fetchall()
    }
    assert fetched_collections == {"C-1"}


def test_run_prefetch_per_granule_failure_does_not_abort_collection(
    tmp_db_path: Path, tmp_path: Path, monkeypatch
) -> None:
    con = connect(tmp_db_path)
    init_schema(con)
    _seed(
        con,
        [
            (
                "C-1",
                1,
                [
                    ("G-a", "s3://b/c1/a.nc", 100),
                    ("G-b", "s3://b/c1/b.nc", 100),
                    ("G-c", "s3://b/c1/c.nc", 100),
                ],
            ),
        ],
    )
    con.close()

    _patch_get_store(
        monkeypatch,
        {
            "s3://b": {
                "c1/a.nc": b"x" * 100,
                "c1/b.nc": b"y" * 100,
                "c1/c.nc": b"z" * 100,
            }
        },
        fail={"c1/b.nc"},
    )

    summary = run_prefetch(
        tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=10_000
    )

    assert summary["granules_fetched"] == 2  # a and c
    assert summary["granules_failed"] == 1  # b
    assert summary["bytes_added"] == 200
    # Collection still counts because at least one granule fetched.
    assert summary["collections_with_fetches"] == 1

    con = connect(tmp_db_path)
    rows = con.execute(
        "SELECT granule_concept_id, status, error FROM prefetch_log "
        "WHERE collection_concept_id = 'C-1' "
        "ORDER BY granule_concept_id"
    ).fetchall()
    by_g = {r[0]: r for r in rows}
    assert by_g["G-a"][1] == "ok"
    assert by_g["G-b"][1] == "fail"
    assert "simulated network failure" in (by_g["G-b"][2] or "")
    assert by_g["G-c"][1] == "ok"


def test_run_prefetch_requires_popularity_rank(
    tmp_db_path: Path, tmp_path: Path, monkeypatch
) -> None:
    con = connect(tmp_db_path)
    init_schema(con)
    # Insert a collection with NO popularity_rank.
    insert_collection(con, "C-1")  # popularity_rank defaults to None
    insert_granule(con, "C-1", "G-a", data_url="s3://b/c1/a.nc", size_bytes=100)
    con.close()

    _patch_get_store(monkeypatch, {"s3://b": {"c1/a.nc": b"x" * 100}})

    with pytest.raises(RuntimeError, match="popularity_rank"):
        run_prefetch(tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=1000)


def test_run_prefetch_is_idempotent_for_cached_granules(
    tmp_db_path: Path, tmp_path: Path, monkeypatch
) -> None:
    """A second prefetch run is a no-op on already-cached granules."""
    con = connect(tmp_db_path)
    init_schema(con)
    _seed(con, [("C-1", 1, [("G-a", "s3://b/c1/a.nc", 100)])])
    con.close()

    _patch_get_store(monkeypatch, {"s3://b": {"c1/a.nc": b"x" * 100}})

    s1 = run_prefetch(tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=10_000)
    s2 = run_prefetch(tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=10_000)

    assert s1["granules_fetched"] == 1
    assert s1["bytes_added"] == 100
    # Second run: nothing newly fetched, no new bytes added.
    assert s2["granules_fetched"] == 0
    assert s2["bytes_added"] == 0

    # Log records the second run as a hit, not a miss.
    con = connect(tmp_db_path)
    statuses = [
        r[0]
        for r in con.execute(
            "SELECT status FROM prefetch_log WHERE action = 'fetch' ORDER BY ts"
        ).fetchall()
    ]
    assert statuses == ["ok", "hit"]


def test_run_prefetch_backfills_size_bytes_when_null(
    tmp_db_path: Path, tmp_path: Path, monkeypatch
) -> None:
    con = connect(tmp_db_path)
    init_schema(con)
    _seed(con, [("C-1", 1, [("G-a", "s3://b/c1/a.nc", None)])])  # null size
    con.close()

    _patch_get_store(monkeypatch, {"s3://b": {"c1/a.nc": b"x" * 250}})

    run_prefetch(tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=10_000)

    con = connect(tmp_db_path)
    row = con.execute(
        "SELECT size_bytes FROM granules WHERE granule_concept_id = 'G-a'"
    ).fetchone()
    assert row is not None
    assert row[0] == 250


def test_run_prefetch_orders_by_popularity_rank(
    tmp_db_path: Path, tmp_path: Path, monkeypatch
) -> None:
    con = connect(tmp_db_path)
    init_schema(con)
    # Insert in non-rank order to confirm rank, not insertion order, drives traversal.
    insert_collection(con, "C-third", popularity_rank=3, usage_score=100)
    insert_collection(con, "C-first", popularity_rank=1, usage_score=300)
    insert_collection(con, "C-second", popularity_rank=2, usage_score=200)
    insert_granule(con, "C-third", "g3", data_url="s3://b/c3.nc", size_bytes=50)
    insert_granule(con, "C-first", "g1", data_url="s3://b/c1.nc", size_bytes=50)
    insert_granule(con, "C-second", "g2", data_url="s3://b/c2.nc", size_bytes=50)
    con.close()

    _patch_get_store(
        monkeypatch,
        {
            "s3://b": {
                "c1.nc": b"x" * 50,
                "c2.nc": b"y" * 50,
                "c3.nc": b"z" * 50,
            }
        },
    )

    run_prefetch(tmp_db_path, cache_dir=tmp_path / "cache", cache_max_bytes=10_000)

    con = connect(tmp_db_path)
    order = [
        r[0]
        for r in con.execute(
            "SELECT collection_concept_id FROM prefetch_log "
            "WHERE action = 'fetch' ORDER BY ts"
        ).fetchall()
    ]
    assert order == ["C-first", "C-second", "C-third"]
