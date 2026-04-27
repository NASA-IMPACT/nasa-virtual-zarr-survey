"""Shared pytest fixtures and test helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import duckdb
import pytest


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """A temporary DuckDB file path."""
    return tmp_path / "survey.duckdb"


@pytest.fixture
def tmp_results_dir(tmp_path: Path) -> Path:
    """A temporary results directory for partitioned Parquet writes."""
    d = tmp_path / "results"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Column-explicit insert helpers
#
# Tests across the suite previously wrote inline positional ``INSERT INTO
# collections VALUES (...)`` statements with 13+ columns of NULL filler. Every
# schema bump forced a sweep through ~30 of them. Using column-explicit INSERTs
# behind keyword-only helpers means: (a) each call site is one readable line,
# and (b) future schema additions are zero-touch in tests because unspecified
# columns just default to NULL.
# ---------------------------------------------------------------------------


def insert_collection(
    con: duckdb.DuckDBPyConnection,
    concept_id: str,
    *,
    short_name: str = "s",
    version: str = "1",
    daac: str = "PODAAC",
    provider: str | None = None,
    format_family: str | None = "NetCDF4",
    format_declared: str | None = "NetCDF-4",
    num_granules: int = 1,
    time_start: datetime | None = None,
    time_end: datetime | None = None,
    processing_level: str | None = "L3",
    skip_reason: str | None = None,
) -> None:
    """Insert one row into the ``collections`` table with sensible defaults.

    Provider defaults to the same value as ``daac``, matching the most common
    test fixture shape. ``discovered_at`` is always ``now()``.
    """
    con.execute(
        """INSERT INTO collections
           (concept_id, short_name, version, daac, provider, format_family,
            format_declared, num_granules, time_start, time_end,
            processing_level, skip_reason, discovered_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())""",
        [
            concept_id,
            short_name,
            version,
            daac,
            provider if provider is not None else daac,
            format_family,
            format_declared,
            num_granules,
            time_start,
            time_end,
            processing_level,
            skip_reason,
        ],
    )


def insert_granule(
    con: duckdb.DuckDBPyConnection,
    collection_concept_id: str,
    granule_concept_id: str,
    *,
    data_url: str | None = "s3://b/file.nc",
    https_url: str | None = None,
    stratification_bin: int = 0,
    n_total_at_sample: int = 0,
    size_bytes: int | None = None,
    sampled_at: datetime | None = None,
    access_mode: str = "direct",
) -> None:
    """Insert one row into the ``granules`` table with sensible defaults.

    ``sampled_at`` defaults to ``now()`` when ``None``.
    """
    if sampled_at is None:
        con.execute(
            """INSERT INTO granules
               (collection_concept_id, granule_concept_id, data_url, https_url,
                stratification_bin, n_total_at_sample, size_bytes, sampled_at, access_mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, now(), ?)""",
            [
                collection_concept_id,
                granule_concept_id,
                data_url,
                https_url,
                stratification_bin,
                n_total_at_sample,
                size_bytes,
                access_mode,
            ],
        )
    else:
        con.execute(
            """INSERT INTO granules
               (collection_concept_id, granule_concept_id, data_url, https_url,
                stratification_bin, n_total_at_sample, size_bytes, sampled_at, access_mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                collection_concept_id,
                granule_concept_id,
                data_url,
                https_url,
                stratification_bin,
                n_total_at_sample,
                size_bytes,
                sampled_at,
                access_mode,
            ],
        )


# ---------------------------------------------------------------------------
# FakeGranule factory
#
# Replaces the half-dozen ad-hoc ``class G:`` definitions that earthaccess
# DataGranule was being mocked with across test_sample.py.
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
      whenever the test needs to round-trip ``umm_json`` through the DB.
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
