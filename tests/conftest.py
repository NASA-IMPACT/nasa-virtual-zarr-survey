"""Shared pytest fixtures and test helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pytest

from vzc.state._io import (
    CollectionRow,
    GranuleRow,
    SurveyState,
    save_state,
    upsert_collections,
    upsert_granules,
)


@pytest.fixture
def tmp_state_path(tmp_path: Path) -> Path:
    """A temporary ``state.json`` path under ``tmp_path``'s canonical layout.

    Returns ``tmp_path/output/state.json`` so tests that ``chdir(tmp_path)``
    and let the CLI use its hardcoded default path resolve to the same file.
    """
    out = tmp_path / "output"
    out.mkdir(exist_ok=True)
    return out / "state.json"


@pytest.fixture
def tmp_results_dir(tmp_path: Path) -> Path:
    """Temporary results directory under ``tmp_path``'s canonical layout."""
    out = tmp_path / "output"
    out.mkdir(exist_ok=True)
    d = out / "results"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Helpers to build SurveyState fixtures
# ---------------------------------------------------------------------------


def make_collection(
    concept_id: str,
    *,
    short_name: str = "s",
    version: str = "1",
    daac: str = "PODAAC",
    provider: str | None = None,
    format_family: str | None = "NetCDF4",
    format_declared: str | None = "NetCDF-4",
    num_granules: int = 1,
    time_start: datetime | str | None = None,
    time_end: datetime | str | None = None,
    processing_level: str | None = "L3",
    skip_reason: str | None = None,
    has_cloud_opendap: bool = False,
    popularity_rank: int | None = None,
    usage_score: int | None = None,
    umm_json: dict[str, Any] | None = None,
) -> CollectionRow:
    """Build a :class:`CollectionRow` with sensible defaults.

    ``provider`` defaults to ``daac``. ``discovered_at`` is always now-UTC.
    """

    def _iso(v: datetime | str | None) -> str | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    return CollectionRow(
        concept_id=concept_id,
        short_name=short_name,
        version=version,
        daac=daac,
        provider=provider if provider is not None else daac,
        format_family=format_family,
        format_declared=format_declared,
        num_granules=num_granules,
        time_start=_iso(time_start),
        time_end=_iso(time_end),
        processing_level=processing_level,
        skip_reason=skip_reason,
        has_cloud_opendap=has_cloud_opendap,
        popularity_rank=popularity_rank,
        usage_score=usage_score,
        discovered_at=datetime.now(timezone.utc).isoformat(),
        umm_json=umm_json,
    )


def make_granule(
    collection_concept_id: str,
    granule_concept_id: str,
    *,
    s3_url: str | None = "s3://b/file.nc",
    https_url: str | None = None,
    dmrpp_granule_url: str | None = None,
    stratification_bin: int = 0,
    n_total_at_sample: int = 0,
    size_bytes: int | None = None,
    sampled_at: datetime | str | None = None,
    umm_json: dict[str, Any] | None = None,
) -> GranuleRow:
    """Build a :class:`GranuleRow` with sensible defaults."""
    if isinstance(sampled_at, datetime):
        sampled = sampled_at.isoformat()
    elif isinstance(sampled_at, str):
        sampled = sampled_at
    else:
        sampled = datetime.now(timezone.utc).isoformat()
    return GranuleRow(
        collection_concept_id=collection_concept_id,
        granule_concept_id=granule_concept_id,
        s3_url=s3_url,
        https_url=https_url,
        dmrpp_granule_url=dmrpp_granule_url,
        stratification_bin=stratification_bin,
        n_total_at_sample=n_total_at_sample,
        size_bytes=size_bytes,
        sampled_at=sampled,
        umm_json=umm_json,
    )


def make_state(
    collections: list[CollectionRow] | None = None,
    granules: list[GranuleRow] | None = None,
    run_meta: dict[str, str] | None = None,
) -> SurveyState:
    """Build a SurveyState fixture from row lists."""
    state = SurveyState()
    if collections:
        upsert_collections(state, collections)
    if granules:
        upsert_granules(state, granules)
    if run_meta:
        state.run_meta.update(run_meta)
    return state


def write_state(state: SurveyState, path: Path) -> Path:
    """Persist a SurveyState to ``path`` (as state.json)."""
    save_state(state, path)
    return path


# ---------------------------------------------------------------------------
# FakeGranule factory
# ---------------------------------------------------------------------------


class _FakeGranule:
    """Minimal stand-in for ``earthaccess.results.DataGranule``.

    Supports ``__getitem__`` for ``meta`` and ``umm`` (matching how production
    code reads granule fields), ``data_links(access=...)`` returning a list of
    URLs, and an optional ``render_dict`` attribute (the canonical accessor
    when present in real earthaccess wrappers).
    """

    def __init__(
        self,
        full_dict: dict[str, Any],
        urls: list[str] | Callable[[str], list[str]],
        with_render_dict: bool,
    ) -> None:
        self._dict = full_dict
        self._urls = urls
        if with_render_dict:
            self.render_dict = full_dict

    def __getitem__(self, key: str) -> Any:
        return self._dict[key]

    def data_links(self, access: str = "direct") -> list[str]:
        if callable(self._urls):
            return self._urls(access)
        return self._urls


def make_fake_granule(
    concept_id: str,
    *,
    umm: dict[str, Any] | None = None,
    urls: list[str] | Callable[[str], list[str]] | None = None,
    with_render_dict: bool = False,
) -> _FakeGranule:
    """Build a fake ``DataGranule`` for tests that mock ``earthaccess.search_data``.

    - ``umm`` defaults to ``{}``; pass a dict to give the fake a non-empty
      ``umm`` payload (e.g. for ``DataGranule.ArchiveAndDistributionInformation``
      probes).
    - ``urls`` is what ``data_links`` returns: a static list, a callable
      ``access -> list[str]`` for tests that need to vary by access mode, or
      ``None`` for the default ``[s3://b/<concept_id>.nc]``.
    - ``with_render_dict`` exposes the full ``{meta, umm}`` as a ``render_dict``
      attribute so ``sample._granule_dict`` takes the canonical path. Set this
      whenever the test needs to round-trip ``umm_json`` through state.
    """
    full_dict: dict[str, Any] = {
        "meta": {"concept-id": concept_id},
        "umm": umm if umm is not None else {},
    }
    return _FakeGranule(
        full_dict=full_dict,
        urls=urls if urls is not None else [f"s3://b/{concept_id}.nc"],
        with_render_dict=with_render_dict,
    )
