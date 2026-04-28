"""Shared TypedDicts for record-shaped dicts that flow between pipeline phases.

The primary on-disk row types (``CollectionRow``, ``GranuleRow``) live in
:mod:`vzc.state._io` as dataclasses. The TypedDicts here cover
ephemeral records that don't persist: pending-work joins, per-collection
verdicts, and per-granule structural fingerprints used by Cubability.
"""

from __future__ import annotations

from datetime import datetime
from typing import TypedDict


class PendingGranule(TypedDict):
    """A pending-work row joined from collections + granules by ``attempt._pending_granules``."""

    collection_concept_id: str
    granule_concept_id: str
    data_url: str | None
    daac: str | None
    provider: str | None
    format_family: str | None


class VerdictRow(TypedDict):
    """Per-collection verdict row produced by ``collection_verdicts``.

    ``parse_verdict``, ``dataset_verdict``, and ``datatree_verdict`` are one of
    'all_pass', 'partial_pass', 'all_fail', 'not_attempted', 'skipped'.

    ``top_bucket`` is the representative failure taxonomy bucket for the
    collection (first parse failure if any, else first dataset failure).
    Empty string means no failure was recorded.
    """

    concept_id: str
    daac: str | None
    format_family: str | None
    skip_reason: str | None
    processing_level: str | None
    parse_verdict: str
    dataset_verdict: str
    datatree_verdict: str
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
    """Collection row subset that ``sample_one_collection`` consumes."""

    concept_id: str
    time_start: datetime | str | None
    time_end: datetime | str | None
    num_granules: int | None
    daac: str | None
    skip_reason: str | None
    has_cloud_opendap: bool
