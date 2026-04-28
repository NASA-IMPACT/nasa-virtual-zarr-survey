"""Phase 2.5 (prefetch): warm the on-disk cache with sampled granules.

HTTPS-only. Walks ``state.collections`` in ascending ``popularity_rank`` and,
for each, downloads every sampled granule via :func:`download_url_to_cache`.
The cap is checked at collection boundaries: a collection that crosses
``cache_max_bytes`` mid-way is allowed to finish, then the run stops. No
deletions. Per-granule failures are logged and skipped; sibling granules
in the same collection are still attempted.

Prefetch is the only writer of the on-disk cache. ``attempt --access
external`` reads from the cache and fails fast on miss.
"""

from __future__ import annotations

import logging
import sys
from typing import Any
from urllib.parse import urlparse

from tqdm.auto import tqdm

from vzc.pipeline._stores import AuthUnavailable, make_https_store
from vzc.pipeline._stores import (
    CacheSizeTracker,
    cache_layout_path,
    download_url_to_cache,
)
from vzc.state._io import (
    GranuleRow,
    SurveyState,
    load_state,
    save_state,
)

_LOGGER = logging.getLogger(__name__)


def prefetch(
    *,
    collection: str | None = None,
    max_granule_bytes: int | None = None,
    cache_max_bytes: int | None = None,
    verbose: bool = False,
) -> dict[str, int]:
    """Pre-warm the cache with sampled granules in popularity order.

    Reads ``output/state.json`` (relative to cwd). Cache directory comes
    from ``NASA_VZ_SURVEY_CACHE_DIR`` (env), default
    ``~/.cache/nasa-virtual-zarr-survey``. ``cache_max_bytes`` defaults to
    ``DEFAULT_CACHE_MAX_BYTES`` (50 GB).

    HTTPS-only — prefetch always reads ``https_url`` from each granule. The
    cap is enforced at collection boundaries: the collection that pushes
    cache size past ``cache_max_bytes`` finishes writing all its granules
    and then the run stops. Cached files are never deleted.

    When ``collection`` is set, prefetch targets just that ``concept_id``
    and skips the popularity-rank requirement — useful for retrying a
    single collection that previously failed.

    When ``max_granule_bytes`` is set, any collection with a sampled
    granule whose ``size_bytes`` exceeds it is skipped wholesale (granules
    with unknown size pass through). Useful for keeping the prefetch cap
    away from a few oversized outliers.

    Returns a summary dict of counts.

    In the default (popularity-ordered) mode, requires ``popularity_rank``
    to be populated; raises :class:`RuntimeError` if no collection carries
    a rank.
    """
    from vzc._config import (
        DEFAULT_CACHE_MAX_BYTES,
        DEFAULT_STATE_PATH,
        cache_dir as _cache_dir,
    )

    cache_path = _cache_dir()
    if cache_max_bytes is None:
        cache_max_bytes = DEFAULT_CACHE_MAX_BYTES
    cache_dir = cache_path  # alias used throughout the body
    state = load_state(DEFAULT_STATE_PATH)

    if collection is None:
        rank_count = sum(1 for c in state.collections if c.popularity_rank is not None)
        if rank_count == 0:
            raise RuntimeError(
                "prefetch requires popularity_rank to be populated; "
                "re-run `discover` with --top or --top-per-provider first, "
                "or pass --collection <concept_id> to target one explicitly."
            )

    rows = _select_collections(state, collection)
    total_collections = len(rows)

    summary: dict[str, int] = {
        "collections_considered": 0,
        "collections_skipped_oversize": 0,
        "collections_with_fetches": 0,
        "granules_fetched": 0,
        "granules_failed": 0,
        "bytes_added": 0,
        "stopped_at_rank": 0,
    }

    cap_tracker = CacheSizeTracker(cache_dir, max_bytes=cache_max_bytes)
    download_tracker = CacheSizeTracker(cache_dir, max_bytes=sys.maxsize)

    outer_bar = tqdm(
        total=total_collections,
        unit="coll",
        desc="prefetch",
        disable=not verbose,
        leave=True,
    )

    def _emit(line: str) -> None:
        if verbose:
            tqdm.write(line, file=sys.stderr)
        else:
            print(line, file=sys.stderr, flush=True)

    https_stores: dict[str, Any] = {}
    state_dirty = False

    for idx, (cid, _provider, rank) in enumerate(rows, 1):
        summary["collections_considered"] += 1
        granules: list[GranuleRow] = sorted(
            state.granules_for(cid),
            key=lambda g: g.stratification_bin,
        )
        if not granules:
            continue

        if max_granule_bytes is not None:
            oversized = [
                g
                for g in granules
                if g.size_bytes is not None and g.size_bytes > max_granule_bytes
            ]
            if oversized:
                summary["collections_skipped_oversize"] += 1
                first = oversized[0]
                # ``oversized`` only contains granules with non-None size_bytes;
                # narrow the type for mypy.
                first_size = first.size_bytes or 0
                _emit(
                    f"[{idx}/{total_collections}] rank={rank} {cid}: "
                    f"skipped — {first.granule_concept_id} is "
                    f"{first_size / 1024**3:.1f} GB > "
                    f"{max_granule_bytes / 1024**3:.1f} GB limit"
                )
                outer_bar.update(1)
                continue

        first_url = next((g.https_url for g in granules if g.https_url), None)
        if first_url is None:
            continue

        if verbose:
            _emit(
                f"[{idx}/{total_collections}] rank={rank} {cid} "
                f"({len(granules)} granules)"
            )

        host_key = f"{urlparse(first_url).scheme}://{urlparse(first_url).netloc}"
        store = https_stores.get(host_key)
        if store is None:
            try:
                store = make_https_store(first_url)
            except AuthUnavailable as e:
                _LOGGER.warning("auth unavailable for %s: %s", cid, e)
                _emit(f"[{idx}/{total_collections}] {cid}: auth unavailable")
                continue
            https_stores[host_key] = store

        any_fetch = False
        ok = hit = fail = 0
        for g in granules:
            url = g.https_url
            if not url:
                continue

            if cache_layout_path(cache_dir, url).exists():
                hit += 1
                if verbose:
                    _emit(f"  hit   {g.granule_concept_id}")
                continue

            inner_bar = tqdm(
                total=g.size_bytes if g.size_bytes and g.size_bytes > 0 else None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=g.granule_concept_id,
                leave=False,
                disable=not verbose,
            )
            try:
                written = download_url_to_cache(
                    store=store,
                    url=url,
                    cache_dir=cache_dir,
                    tracker=download_tracker,
                    on_chunk=inner_bar.update,
                )
            except Exception as e:
                inner_bar.close()
                summary["granules_failed"] += 1
                fail += 1
                if verbose:
                    _emit(f"  fail  {g.granule_concept_id}: {repr(e)[:160]}")
                continue
            inner_bar.close()

            if written is None:
                if verbose:
                    _emit(
                        f"  miss  {g.granule_concept_id}: write skipped "
                        "(cap or local error)"
                    )
                continue

            local_size = cache_layout_path(cache_dir, url).stat().st_size
            if g.size_bytes is None:
                g.size_bytes = local_size
                state_dirty = True
            cap_tracker.add(local_size)
            ok += 1
            if verbose:
                _emit(f"  ok    {g.granule_concept_id} ({local_size / 1024**2:.1f} MB)")
            summary["granules_fetched"] += 1
            summary["bytes_added"] += local_size
            any_fetch = True

        if any_fetch:
            summary["collections_with_fetches"] += 1

        cache_gb = cap_tracker.current_size / 1024**3
        cap_gb = cache_max_bytes / 1024**3
        _emit(
            f"[{idx}/{total_collections}] rank={rank} {cid}: "
            f"{ok} ok, {hit} hit, {fail} fail "
            f"(cache {cache_gb:.1f}/{cap_gb:.1f} GB)"
        )
        outer_bar.update(1)

        if cap_tracker.current_size >= cache_max_bytes:
            summary["stopped_at_rank"] = int(rank) if rank is not None else 0
            _LOGGER.info(
                "prefetch stopped after rank %s (%s): cache %.1f / %.1f GB cap "
                "(overshoot allowed at collection boundary)",
                rank,
                cid,
                cache_gb,
                cap_gb,
            )
            break

    outer_bar.close()

    if state_dirty:
        save_state(state, DEFAULT_STATE_PATH)

    return summary


def _select_collections(
    state: SurveyState, collection: str | None
) -> list[tuple[str, str | None, int | None]]:
    """Pick collections (concept_id, provider, popularity_rank) in walk order.

    With ``collection`` set: just that one (must have sampled granules).
    Otherwise: all ranked, array-like collections that have sampled granules,
    sorted by ``popularity_rank``.
    """
    sampled = {g.collection_concept_id for g in state.granules}
    if collection is not None:
        coll = state.collection(collection)
        if coll is None or collection not in sampled:
            raise RuntimeError(
                f"--collection {collection!r} matched no row with sampled "
                "granules. Check the concept_id and that `sample` has run."
            )
        return [(coll.concept_id, coll.provider, coll.popularity_rank)]

    eligible = [
        c
        for c in state.collections
        if c.popularity_rank is not None
        and c.skip_reason in (None, "format_unknown")
        and c.concept_id in sampled
    ]
    eligible.sort(key=lambda c: c.popularity_rank or 0)
    return [(c.concept_id, c.provider, c.popularity_rank) for c in eligible]
