"""Phase 2.5 (prefetch): warm the on-disk cache with sampled granules in popularity order.

Walks ``collections`` in ascending ``popularity_rank`` and, for each, fetches every
sampled granule through the same ``DiskCachingReadableStore`` that ``attempt`` and
``snapshot`` use. The cap is checked at collection boundaries: a collection that
crosses ``cache_max_bytes`` mid-way is allowed to finish, then the run stops. No
deletions. Per-granule failures are logged and skipped; sibling granules in the
same collection are still attempted.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from tqdm.auto import tqdm

from nasa_virtual_zarr_survey.auth import AuthUnavailable, StoreCache
from nasa_virtual_zarr_survey.cache import DiskCachingReadableStore
from nasa_virtual_zarr_survey.db import connect, init_schema

_LOGGER = logging.getLogger(__name__)


def _url_path(url: str) -> str:
    """Path component of *url* with the leading slash stripped.

    The wrapped store's prefix is ``scheme://host`` (S3 bucket as host), so
    ``store.get(path)`` expects everything after that.
    """
    return urlparse(url).path.lstrip("/")


def _log(
    con,
    collection_id: str,
    granule_id: str,
    action: str,
    status: str,
    size_bytes: int | None,
    error: str | None,
) -> None:
    con.execute(
        "INSERT INTO prefetch_log "
        "(collection_concept_id, granule_concept_id, action, status, "
        "size_bytes, error, ts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            collection_id,
            granule_id,
            action,
            status,
            size_bytes,
            error,
            datetime.now(timezone.utc),
        ],
    )


def run_prefetch(
    db_path: Path | str,
    *,
    cache_dir: Path,
    cache_max_bytes: int,
    access: Literal["direct", "external"] = "direct",
    verbose: bool = False,
    collection: str | None = None,
    max_granule_bytes: int | None = None,
) -> dict[str, int]:
    """Pre-warm the cache with sampled granules in popularity order.

    The cap is enforced at collection boundaries: the collection that pushes
    cache size past ``cache_max_bytes`` finishes writing all its granules and
    then the run stops. Cached files are never deleted.

    Per-granule failures are logged but don't abort the collection — the next
    granule in the same collection is still attempted.

    Per-collection progress lines are printed to stderr (matching ``attempt``).
    Pass ``verbose=True`` to also print per-granule ``ok``/``hit``/``fail``
    lines.

    When ``collection`` is set, prefetch targets just that
    ``concept_id`` and skips the popularity-rank requirement — useful for
    re-trying a single collection that previously failed.

    When ``max_granule_bytes`` is set, any collection with a sampled granule
    whose ``size_bytes`` exceeds it is skipped wholesale and recorded in
    ``prefetch_log`` with ``action='skip'``. Granules with unknown size
    (NULL) pass through.

    Returns a summary dict: counts of collections considered/skipped/with-any-fetch,
    granules fetched/failed, bytes added, and the rank at which the run
    stopped (0 if it walked through every ranked collection).

    In the default (popularity-ordered) mode, requires popularity_rank to be
    populated; raises ``RuntimeError`` if no collection carries a rank —
    meaning ``discover`` was not run with ``--top`` or ``--top-per-provider``.
    """
    con = connect(db_path)
    init_schema(con)

    if collection is None:
        rank_count = (
            con.execute(
                "SELECT count(*) FROM collections WHERE popularity_rank IS NOT NULL"
            ).fetchone()
            or (0,)
        )[0]
        if rank_count == 0:
            raise RuntimeError(
                "prefetch requires popularity_rank to be populated; "
                "re-run `discover` with --top or --top-per-provider first, "
                "or pass --collection <concept_id> to target one explicitly."
            )

    # The wrapper enforces its own cap by refusing to write objects that would
    # cross it. We want overshoot tolerance up to one collection's worth, so we
    # disable the wrapper's cap and budget the surplus ourselves at collection
    # boundaries below.
    cache = StoreCache(access=access, cache_dir=cache_dir, cache_max_bytes=sys.maxsize)
    tracker = cache.tracker
    if tracker is None:
        raise RuntimeError("prefetch requires a cache_dir (caching is mandatory).")

    if collection is not None:
        rows = con.execute(
            """
            SELECT c.concept_id, c.provider, c.popularity_rank
            FROM collections c
            WHERE c.concept_id = ?
              AND EXISTS (
                  SELECT 1 FROM granules g
                  WHERE g.collection_concept_id = c.concept_id
              )
            """,
            [collection],
        ).fetchall()
        if not rows:
            raise RuntimeError(
                f"--collection {collection!r} matched no row with sampled "
                "granules. Check the concept_id and that `sample` has run."
            )
    else:
        rows = con.execute(
            """
            SELECT c.concept_id, c.provider, c.popularity_rank
            FROM collections c
            WHERE c.popularity_rank IS NOT NULL
              AND (c.skip_reason IS NULL OR c.skip_reason = 'format_unknown')
              AND EXISTS (
                  SELECT 1 FROM granules g
                  WHERE g.collection_concept_id = c.concept_id
              )
            ORDER BY c.popularity_rank ASC
            """
        ).fetchall()
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

    # tqdm prints to stderr by default — same stream as our per-collection
    # summary line, so they interleave cleanly without colliding with stdout.
    outer_bar = tqdm(
        total=total_collections,
        unit="coll",
        desc="prefetch",
        disable=not verbose,
        leave=True,
    )

    def _emit(line: str) -> None:
        # tqdm.write avoids stomping the active progress bar.
        if verbose:
            tqdm.write(line, file=sys.stderr)
        else:
            print(line, file=sys.stderr, flush=True)

    for idx, (cid, provider, rank) in enumerate(rows, 1):
        summary["collections_considered"] += 1
        granules = con.execute(
            "SELECT granule_concept_id, data_url, size_bytes "
            "FROM granules WHERE collection_concept_id = ? "
            "ORDER BY stratification_bin",
            [cid],
        ).fetchall()
        if not granules:
            continue

        if max_granule_bytes is not None:
            oversized = [
                (gid, size)
                for gid, _url, size in granules
                if size is not None and size > max_granule_bytes
            ]
            if oversized:
                first_gid, first_size = oversized[0]
                _log(
                    con,
                    cid,
                    first_gid,
                    "skip",
                    "oversize",
                    first_size,
                    f">{max_granule_bytes} bytes",
                )
                summary["collections_skipped_oversize"] += 1
                _emit(
                    f"[{idx}/{total_collections}] rank={rank} {cid}: "
                    f"skipped — {first_gid} is {first_size / 1024**3:.1f} GB "
                    f"> {max_granule_bytes / 1024**3:.1f} GB limit"
                )
                outer_bar.update(1)
                continue

        first_url = next((g[1] for g in granules if g[1]), None)
        if first_url is None:
            continue

        if verbose:
            _emit(
                f"[{idx}/{total_collections}] rank={rank} {cid} ({len(granules)} granules)"
            )

        try:
            store = cache.get_store(provider=provider, url=first_url)
        except AuthUnavailable as e:
            _LOGGER.warning("auth unavailable for %s: %s", cid, e)
            for gid, _url, _size in granules:
                _log(con, cid, gid, "auth", "fail", None, str(e))
            _emit(f"[{idx}/{total_collections}] {cid}: auth unavailable")
            continue
        if not isinstance(store, DiskCachingReadableStore):  # defensive
            raise RuntimeError(
                "prefetch needs a cache-wrapped store; cache_dir was not honored."
            )

        any_fetch = False
        ok = hit = fail = 0
        for gid, url, size in granules:
            if not url:
                _log(con, cid, gid, "fetch", "skip", size, "missing data_url")
                continue
            path = _url_path(url)
            already = store.is_cached(path)
            if already:
                _log(con, cid, gid, "fetch", "hit", size, None)
                hit += 1
                if verbose:
                    _emit(f"  hit   {gid}")
                continue

            inner_bar = tqdm(
                total=size if size and size > 0 else None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=gid,
                leave=False,
                disable=not verbose,
            )
            try:
                cached_ok = store.prefetch_to_cache(path, on_chunk=inner_bar.update)
            except Exception as e:
                inner_bar.close()
                _log(con, cid, gid, "fetch", "fail", size, repr(e))
                summary["granules_failed"] += 1
                fail += 1
                if verbose:
                    _emit(f"  fail  {gid}: {repr(e)[:160]}")
                continue
            inner_bar.close()

            cached = store.cached_path(path)
            if not cached_ok or cached is None:
                _log(con, cid, gid, "fetch", "uncached", size, None)
                if verbose:
                    _emit(f"  miss  {gid}: write skipped (cap or local error)")
                continue
            local_size = cached.stat().st_size
            if size is None:
                con.execute(
                    "UPDATE granules SET size_bytes = ? "
                    "WHERE collection_concept_id = ? AND granule_concept_id = ?",
                    [local_size, cid, gid],
                )
            _log(con, cid, gid, "fetch", "ok", local_size, None)
            ok += 1
            if verbose:
                _emit(f"  ok    {gid} ({local_size / 1024**2:.1f} MB)")
            summary["granules_fetched"] += 1
            summary["bytes_added"] += local_size
            any_fetch = True

        if any_fetch:
            summary["collections_with_fetches"] += 1

        cache_gb = tracker.current_size / 1024**3
        cap_gb = cache_max_bytes / 1024**3
        _emit(
            f"[{idx}/{total_collections}] rank={rank} {cid}: "
            f"{ok} ok, {hit} hit, {fail} fail "
            f"(cache {cache_gb:.1f}/{cap_gb:.1f} GB)"
        )
        outer_bar.update(1)

        if tracker.current_size >= cache_max_bytes:
            summary["stopped_at_rank"] = int(rank)
            _LOGGER.info(
                "prefetch stopped after rank %d (%s): cache %.1f / %.1f GB cap "
                "(overshoot allowed at collection boundary)",
                rank,
                cid,
                cache_gb,
                cap_gb,
            )
            break

    outer_bar.close()
    return summary
