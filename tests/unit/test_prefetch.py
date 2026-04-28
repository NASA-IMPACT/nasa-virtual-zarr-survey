"""Unit tests for prefetch (Phase 2.5)."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pytest

from vzc.pipeline._prefetch import prefetch
from vzc.state._io import load_state, save_state
from tests.conftest import make_collection, make_granule, make_state


@pytest.fixture(autouse=True)
def _reset_cap_warning(monkeypatch):
    monkeypatch.setattr("vzc.pipeline._stores._CAP_WARNING_EMITTED", False)


class _FakeStream:
    def __init__(self, chunks: Iterable[bytes]) -> None:
        self._chunks = list(chunks)

    def stream(self, min_chunk_size: int = 0):
        yield from self._chunks


class _FakeStore:
    def __init__(self, payloads: dict[str, bytes], failures: set[str]) -> None:
        self._payloads = payloads
        self._failures = failures

    def head(self, path: str) -> dict[str, int]:
        return {"size": len(self._payloads[path])}

    def get(self, path: str):
        if path in self._failures:
            raise RuntimeError(f"simulated network failure for {path}")
        return _FakeStream([self._payloads[path]])


def _patch_make_https_store(
    monkeypatch,
    payloads: dict[str, bytes],
    *,
    failures: set[str] | None = None,
) -> None:
    fail_set = failures or set()
    store = _FakeStore(payloads, fail_set)

    def fake_make_https_store(url, *, token=None):
        return store

    monkeypatch.setattr(
        "vzc.pipeline._prefetch.make_https_store", fake_make_https_store
    )


def _seed_state(
    state_path: Path,
    ranked: list[tuple[str, int | None, list[tuple[str, str, int | None]]]],
) -> None:
    """Seed state.json with ``ranked = [(concept_id, rank, [(granule_id, url, size)])]``."""
    collections = []
    granules = []
    for cid, rank, gs in ranked:
        collections.append(
            make_collection(
                cid,
                popularity_rank=rank,
                usage_score=(1000 - rank) if rank is not None else None,
            )
        )
        for i, (gid, url, size) in enumerate(gs):
            granules.append(
                make_granule(
                    cid,
                    gid,
                    s3_url=None,
                    https_url=url,
                    size_bytes=size,
                    stratification_bin=i,
                )
            )
    save_state(make_state(collections=collections, granules=granules), state_path)


def test_run_prefetch_happy_path(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    _seed_state(
        tmp_state_path,
        [
            (
                "C-1",
                1,
                [
                    ("G-1a", "https://h/c1/a.nc", 100),
                    ("G-1b", "https://h/c1/b.nc", 200),
                ],
            ),
            ("C-2", 2, [("G-2a", "https://h/c2/a.nc", 150)]),
        ],
    )

    _patch_make_https_store(
        monkeypatch,
        {
            "c1/a.nc": b"x" * 100,
            "c1/b.nc": b"y" * 200,
            "c2/a.nc": b"z" * 150,
        },
    )

    summary = prefetch(cache_max_bytes=10_000)

    assert summary["collections_considered"] == 2
    assert summary["collections_with_fetches"] == 2
    assert summary["granules_fetched"] == 3
    assert summary["granules_failed"] == 0
    assert summary["bytes_added"] == 450
    assert summary["stopped_at_rank"] == 0


def test_run_prefetch_stops_at_cap_with_overshoot(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    """First collection finishes even if it crosses the cap; second is not entered."""
    _seed_state(
        tmp_state_path,
        [
            (
                "C-1",
                1,
                [
                    ("G-1a", "https://h/c1/a.nc", 1000),
                    ("G-1b", "https://h/c1/b.nc", 500),
                ],
            ),
            ("C-2", 2, [("G-2a", "https://h/c2/a.nc", 100)]),
        ],
    )

    _patch_make_https_store(
        monkeypatch,
        {
            "c1/a.nc": b"x" * 1000,
            "c1/b.nc": b"y" * 500,
            "c2/a.nc": b"z" * 100,
        },
    )

    summary = prefetch(cache_max_bytes=1000)

    assert summary["granules_fetched"] == 2
    assert summary["bytes_added"] == 1500
    assert summary["stopped_at_rank"] == 1


def test_run_prefetch_per_granule_failure_does_not_abort_collection(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    _seed_state(
        tmp_state_path,
        [
            (
                "C-1",
                1,
                [
                    ("G-a", "https://h/c1/a.nc", 100),
                    ("G-b", "https://h/c1/b.nc", 100),
                    ("G-c", "https://h/c1/c.nc", 100),
                ],
            ),
        ],
    )

    _patch_make_https_store(
        monkeypatch,
        {
            "c1/a.nc": b"x" * 100,
            "c1/b.nc": b"y" * 100,
            "c1/c.nc": b"z" * 100,
        },
        failures={"c1/b.nc"},
    )

    summary = prefetch(cache_max_bytes=10_000)

    assert summary["granules_fetched"] == 2
    assert summary["granules_failed"] == 1
    assert summary["bytes_added"] == 200
    assert summary["collections_with_fetches"] == 1


def test_run_prefetch_collection_filter_targets_one_collection(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    state = make_state(
        collections=[
            make_collection("C-ranked", popularity_rank=1, usage_score=999),
            make_collection("C-other"),
        ],
        granules=[
            make_granule(
                "C-ranked",
                "g-r",
                s3_url=None,
                https_url="https://h/r.nc",
                size_bytes=50,
            ),
            make_granule(
                "C-other", "g-o", s3_url=None, https_url="https://h/o.nc", size_bytes=50
            ),
        ],
    )
    save_state(state, tmp_state_path)

    _patch_make_https_store(
        monkeypatch,
        {"r.nc": b"r" * 50, "o.nc": b"o" * 50},
    )

    summary = prefetch(
        cache_max_bytes=10_000,
        collection="C-other",
    )
    assert summary["granules_fetched"] == 1
    assert summary["bytes_added"] == 50


def test_run_prefetch_collection_filter_unknown_id_raises(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    state = make_state(
        collections=[make_collection("C-1", popularity_rank=1)],
        granules=[
            make_granule(
                "C-1", "g-1", s3_url=None, https_url="https://h/x.nc", size_bytes=10
            )
        ],
    )
    save_state(state, tmp_state_path)
    _patch_make_https_store(monkeypatch, {"x.nc": b"x" * 10})

    with pytest.raises(RuntimeError, match="matched no row"):
        prefetch(
            cache_max_bytes=1000,
            collection="C-does-not-exist",
        )


def test_run_prefetch_requires_popularity_rank(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    state = make_state(
        collections=[make_collection("C-1")],  # no popularity_rank
        granules=[
            make_granule(
                "C-1", "G-a", s3_url=None, https_url="https://h/c1/a.nc", size_bytes=100
            )
        ],
    )
    save_state(state, tmp_state_path)

    _patch_make_https_store(monkeypatch, {"c1/a.nc": b"x" * 100})

    with pytest.raises(RuntimeError, match="popularity_rank"):
        prefetch(cache_max_bytes=1000)


def test_run_prefetch_is_idempotent_for_cached_granules(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    """A second prefetch run is a no-op on already-cached granules."""
    _seed_state(tmp_state_path, [("C-1", 1, [("G-a", "https://h/c1/a.nc", 100)])])

    _patch_make_https_store(monkeypatch, {"c1/a.nc": b"x" * 100})

    s1 = prefetch(cache_max_bytes=10_000)
    s2 = prefetch(cache_max_bytes=10_000)

    assert s1["granules_fetched"] == 1
    assert s1["bytes_added"] == 100
    assert s2["granules_fetched"] == 0
    assert s2["bytes_added"] == 0


def test_run_prefetch_backfills_size_bytes_when_null(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    _seed_state(tmp_state_path, [("C-1", 1, [("G-a", "https://h/c1/a.nc", None)])])

    _patch_make_https_store(monkeypatch, {"c1/a.nc": b"x" * 250})

    prefetch(cache_max_bytes=10_000)

    state = load_state(tmp_state_path)
    g = next(g for g in state.granules if g.granule_concept_id == "G-a")
    assert g.size_bytes == 250


def test_run_prefetch_orders_by_popularity_rank(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    state = make_state(
        collections=[
            make_collection("C-third", popularity_rank=3, usage_score=100),
            make_collection("C-first", popularity_rank=1, usage_score=300),
            make_collection("C-second", popularity_rank=2, usage_score=200),
        ],
        granules=[
            make_granule(
                "C-third", "g3", s3_url=None, https_url="https://h/c3.nc", size_bytes=50
            ),
            make_granule(
                "C-first", "g1", s3_url=None, https_url="https://h/c1.nc", size_bytes=50
            ),
            make_granule(
                "C-second",
                "g2",
                s3_url=None,
                https_url="https://h/c2.nc",
                size_bytes=50,
            ),
        ],
    )
    save_state(state, tmp_state_path)

    fetched_order: list[str] = []

    class _RecordingStore(_FakeStore):
        def __init__(self, payloads, failures, host_to_coll):
            super().__init__(payloads, failures)
            self._host_to_coll = host_to_coll

        def get(self, path):
            for hostpath, cid in self._host_to_coll.items():
                if path == hostpath:
                    fetched_order.append(cid)
                    break
            return super().get(path)

    store = _RecordingStore(
        {"c1.nc": b"x" * 50, "c2.nc": b"y" * 50, "c3.nc": b"z" * 50},
        set(),
        {"c1.nc": "C-first", "c2.nc": "C-second", "c3.nc": "C-third"},
    )

    monkeypatch.setattr(
        "vzc.pipeline._prefetch.make_https_store",
        lambda url, *, token=None: store,
    )

    prefetch(cache_max_bytes=10_000)

    assert fetched_order == ["C-first", "C-second", "C-third"]


def test_run_prefetch_max_granule_bytes_skips_oversize_collection(
    tmp_state_path: Path, tmp_path: Path, monkeypatch
) -> None:
    """Collections with any sampled granule above the per-granule cap are skipped wholesale."""
    monkeypatch.chdir(tmp_state_path.parent.parent)
    monkeypatch.setenv("NASA_VZ_SURVEY_CACHE_DIR", str(tmp_path / "cache"))
    _seed_state(
        tmp_state_path,
        [
            (
                "C-big",
                1,
                [
                    ("G-big", "https://h/big.nc", 5 * 1024**3),
                    ("G-tiny", "https://h/big-tiny.nc", 100),
                ],
            ),
            ("C-small", 2, [("G-s", "https://h/small.nc", 100)]),
        ],
    )

    _patch_make_https_store(
        monkeypatch,
        {"big.nc": b"x" * 100, "big-tiny.nc": b"y" * 100, "small.nc": b"z" * 100},
    )

    summary = prefetch(
        cache_max_bytes=10_000,
        max_granule_bytes=1 * 1024**3,
    )
    assert summary["collections_skipped_oversize"] == 1
    assert summary["granules_fetched"] == 1
