"""Shared TypedDicts for record-shaped dicts that flow between pipeline phases."""

from __future__ import annotations

from datetime import datetime
from typing import TypedDict


class CollectionRow(TypedDict):
    """A row of the DuckDB ``collections`` table, produced by ``collection_row_from_umm``."""

    concept_id: str | None
    short_name: str | None
    version: str | None
    daac: str | None
    provider: str | None
    format_family: str | None
    format_declared: str | None
    num_granules: int | None
    time_start: datetime | None
    time_end: datetime | None
    processing_level: str | None
    skip_reason: str | None
    discovered_at: datetime


class GranuleInfo(TypedDict):
    """A row of the DuckDB ``granules`` table, produced by ``sample_one_collection``."""

    collection_concept_id: str
    granule_concept_id: str
    data_url: str | None
    temporal_bin: int
    size_bytes: int | None
    sampled_at: datetime
    stratified: bool


class PendingGranule(TypedDict):
    """A pending-work row joined from ``collections`` and ``granules`` by ``_pending_granules``."""

    collection_concept_id: str
    granule_concept_id: str
    data_url: str | None
    daac: str | None
    provider: str | None
    format_family: str | None
    stratified: bool | None


class VerdictRow(TypedDict):
    """Per-collection verdict row produced by ``collection_verdicts``.

    ``parse_verdict`` and ``dataset_verdict`` are one of
    'all_pass', 'partial_pass', 'all_fail', 'not_attempted', 'skipped'.

    ``top_bucket`` is the representative failure taxonomy bucket for the
    collection (first parse failure if any, else first dataset failure).
    Empty string means no failure was recorded.
    """

    concept_id: str
    daac: str | None
    format_family: str | None
    skip_reason: str | None
    stratified: bool | None
    parse_verdict: str
    dataset_verdict: str
    top_bucket: str


class VarInfo(TypedDict):
    """Per-data-variable fingerprint entry used by the cubability check."""

    dtype: str
    dims: list[str]
    chunks: list[int] | None
    fill_value: str | None
    codecs: list[str]


class CoordInfo(TypedDict):
    """Per-coord fingerprint entry used by the cubability check."""

    dtype: str
    dims: list[str]
    shape: list[int]
    values_hash: str
    min: str | int | float | None
    max: str | int | float | None


class Fingerprint(TypedDict):
    """Per-granule fingerprint shape captured after Phase 4 succeeds."""

    dims: dict[str, int]
    data_vars: dict[str, VarInfo]
    coords: dict[str, CoordInfo]


class SampleCollection(TypedDict):
    """Collection row subset that ``sample_one_collection`` and ``run_sample`` use."""

    concept_id: str
    time_start: datetime | None
    time_end: datetime | None
    num_granules: int | None
    daac: str | None
    skip_reason: str | None
