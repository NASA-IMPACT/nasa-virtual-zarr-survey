"""Survey state on disk: collections + granules + run_meta in one JSON file.

The same shape is committed as ``config/locked_sample.json`` for
reproducibility — locked samples are a hand-trimmed subset of the full
state (no ``umm_json``, etc.); both files load through the same dataclasses.

Read-side queries that used to be SQL (e.g.
``SELECT * FROM collections WHERE skip_reason IS NULL``) are now Python list
comprehensions over ``SurveyState.collections``. Cross-table queries against
the per-attempt Parquet log live in ``vzc.state._results``.

Schema v1 corresponds to the post-DuckDB-drop world. Older DuckDB checkpoints
are not migrated; operators regenerate via ``discover && sample``.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Literal

SCHEMA_VERSION = 1

AccessMode = Literal["direct", "external"]


@dataclass
class CollectionRow:
    concept_id: str
    short_name: str | None = None
    version: str | None = None
    daac: str | None = None
    provider: str | None = None
    format_family: str | None = None
    format_declared: str | None = None
    num_granules: int | None = None
    time_start: str | None = None  # ISO datetime
    time_end: str | None = None
    processing_level: str | None = None
    skip_reason: str | None = None
    has_cloud_opendap: bool = False
    popularity_rank: int | None = None
    usage_score: int | None = None
    discovered_at: str | None = None
    umm_json: dict[str, Any] | None = None


@dataclass
class GranuleRow:
    collection_concept_id: str
    granule_concept_id: str
    s3_url: str | None = None
    https_url: str | None = None
    dmrpp_granule_url: str | None = None
    stratification_bin: int = 0
    n_total_at_sample: int = 0
    size_bytes: int | None = None
    sampled_at: str | None = None
    umm_json: dict[str, Any] | None = None

    def url_for(self, access: AccessMode) -> str | None:
        """Pick ``s3_url`` (direct) or ``https_url`` (external)."""
        return self.s3_url if access == "direct" else self.https_url


@dataclass
class SurveyState:
    """The full survey-state document.

    ``collections`` and ``granules`` are flat lists; ``run_meta`` is a small
    string-valued dict (today's only key is ``sampling_mode``).
    """

    collections: list[CollectionRow] = field(default_factory=list)
    granules: list[GranuleRow] = field(default_factory=list)
    run_meta: dict[str, str] = field(default_factory=dict)

    # ---- Convenience accessors (replace common SQL queries) -------------

    def collection(self, concept_id: str) -> CollectionRow | None:
        return next((c for c in self.collections if c.concept_id == concept_id), None)

    def collections_by_id(self) -> dict[str, CollectionRow]:
        return {c.concept_id: c for c in self.collections}

    def granules_for(self, concept_id: str) -> list[GranuleRow]:
        return [g for g in self.granules if g.collection_concept_id == concept_id]

    def array_like_collections(self) -> list[CollectionRow]:
        """Collections that survived discover's format-family filter."""
        return [c for c in self.collections if c.skip_reason is None]


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------


def load_state(path: Path | str = Path("output/state.json")) -> SurveyState:
    """Read a ``state.json`` (or ``locked_sample.json``) into a SurveyState.

    Missing file → empty state, so ``discover`` can start from scratch.
    Schema mismatch → ``ValueError``.
    """
    p = Path(path)
    if not p.exists():
        return SurveyState()
    data = json.loads(p.read_text())
    version = data.get("schema_version")
    if version != SCHEMA_VERSION:
        raise ValueError(
            f"state schema_version {version!r} doesn't match the current "
            f"SCHEMA_VERSION ({SCHEMA_VERSION}). Delete the file and re-run "
            "`discover && sample`."
        )
    coll_fields = {f.name for f in fields(CollectionRow)}
    gran_fields = {f.name for f in fields(GranuleRow)}
    return SurveyState(
        collections=[
            CollectionRow(**{k: v for k, v in c.items() if k in coll_fields})
            for c in data.get("collections", [])
        ],
        granules=[
            GranuleRow(**{k: v for k, v in g.items() if k in gran_fields})
            for g in data.get("granules", [])
        ],
        run_meta=dict(data.get("run_meta", {})),
    )


def save_state(
    state: SurveyState,
    path: Path | str = Path("output/state.json"),
) -> None:
    """Write a SurveyState to disk as JSON. Creates parent dirs."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "collections": [asdict(c) for c in state.collections],
        "granules": [asdict(g) for g in state.granules],
        "run_meta": dict(state.run_meta),
    }
    p.write_text(json.dumps(payload, indent=2, default=_json_default))


def _json_default(o: Any) -> Any:
    """JSON encoder for the few non-builtin types we serialize."""
    if isinstance(o, datetime):
        return o.isoformat()
    if hasattr(o, "isoformat"):  # date
        return o.isoformat()
    raise TypeError(f"not serializable: {type(o).__name__}")


# ---------------------------------------------------------------------------
# Append helpers (used by discover and sample)
# ---------------------------------------------------------------------------


def upsert_collections(
    state: SurveyState, rows: Iterable[CollectionRow | dict[str, Any]]
) -> None:
    """Append or replace collection rows by ``concept_id`` (in-place)."""
    coll_fields = {f.name for f in fields(CollectionRow)}
    by_id = {c.concept_id: c for c in state.collections}
    for row in rows:
        if isinstance(row, CollectionRow):
            c = row
        else:
            c = CollectionRow(**{k: v for k, v in row.items() if k in coll_fields})
        by_id[c.concept_id] = c
    state.collections = list(by_id.values())


def upsert_granules(
    state: SurveyState, rows: Iterable[GranuleRow | dict[str, Any]]
) -> None:
    """Append or replace granule rows by ``(collection, granule)`` pair."""
    gran_fields = {f.name for f in fields(GranuleRow)}
    by_key = {
        (g.collection_concept_id, g.granule_concept_id): g for g in state.granules
    }
    for row in rows:
        if isinstance(row, GranuleRow):
            g = row
        else:
            g = GranuleRow(**{k: v for k, v in row.items() if k in gran_fields})
        by_key[(g.collection_concept_id, g.granule_concept_id)] = g
    state.granules = list(by_key.values())


def delete_granules_for_collection(state: SurveyState, concept_id: str) -> int:
    """Drop every granule row tied to ``concept_id``. Returns the count removed."""
    before = len(state.granules)
    state.granules = [
        g for g in state.granules if g.collection_concept_id != concept_id
    ]
    return before - len(state.granules)


# ---------------------------------------------------------------------------
# Pending granules (replaces the SQL antijoin in attempt._pending_granules)
# ---------------------------------------------------------------------------


def pending_granules(
    state: SurveyState,
    results_dir: Path,
    *,
    only_collection: str | None = None,
) -> list[GranuleRow]:
    """Return granule rows that have no Parquet result yet.

    Reads ``results_dir`` for already-attempted ``(collection, granule)``
    pairs via :mod:`vzc.state._results`. Skips collections with a
    non-empty ``skip_reason``. Optional ``only_collection`` filter.
    """
    from vzc.state._results import attempted_pairs

    coll_by_id = state.collections_by_id()
    done = attempted_pairs(results_dir)

    out: list[GranuleRow] = []
    for g in state.granules:
        coll = coll_by_id.get(g.collection_concept_id)
        if coll is None or coll.skip_reason is not None:
            continue
        if only_collection is not None and coll.concept_id != only_collection:
            continue
        if (g.collection_concept_id, g.granule_concept_id) in done:
            continue
        out.append(g)

    out.sort(
        key=lambda g: (
            (coll_by_id[g.collection_concept_id].daac or ""),
            g.collection_concept_id,
            g.stratification_bin,
        )
    )
    return out
