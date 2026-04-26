"""Unit tests for nasa_virtual_zarr_survey.probe."""

from __future__ import annotations

import py_compile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import click
import pytest

from nasa_virtual_zarr_survey.formats import FormatFamily
from nasa_virtual_zarr_survey.probe import (
    ProbeTarget,
    generate_script,
    resolve_target,
)


def _assert_compiles(source: str) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(source)
        path = Path(f.name)
    try:
        py_compile.compile(str(path), doraise=True)
    finally:
        path.unlink()


# ---------------------------------------------------------------------------
# Fake earthaccess granule + dataset records for monkeypatching search_*
# ---------------------------------------------------------------------------


class _FakeGranule(dict):
    """Minimal stand-in for earthaccess.results.DataGranule."""

    def __init__(
        self,
        *,
        concept_id: str,
        provider: str,
        direct_url: str,
        external_url: str | None = None,
        declared_format: str | None = None,
    ) -> None:
        super().__init__()
        self["meta"] = {
            "concept-id": concept_id,
            "provider-id": provider,
            "collection-concept-id": "C-from-cmr",
        }
        umm: dict[str, Any] = {}
        if declared_format is not None:
            umm["DataGranule"] = {
                "ArchiveAndDistributionInformation": [{"Format": declared_format}]
            }
        self["umm"] = umm
        self._direct = direct_url
        self._external = external_url

    def data_links(self, *, access: str = "direct") -> list[str]:
        if access == "direct":
            return [self._direct] if self._direct else []
        return [self._external] if self._external else []


class _StrictMock:
    """A callable that fails the test if invoked."""

    def __init__(self, label: str) -> None:
        self.label = label

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"unexpected call to {self.label}({args!r}, {kwargs!r})")


# ---------------------------------------------------------------------------
# resolve_target — granule input
# ---------------------------------------------------------------------------


def test_resolve_target_granule_db_hit(tmp_db_path: Path, monkeypatch) -> None:
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from tests.conftest import insert_collection, insert_granule

    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C-DB", short_name="n")
    insert_granule(
        con,
        "C-DB",
        "G-DB",
        data_url="s3://bucket/path/file.nc",
        sampled_at=datetime.now(timezone.utc),
    )
    con.close()

    # Strict: any earthaccess call fails.
    import earthaccess

    monkeypatch.setattr(earthaccess, "search_data", _StrictMock("search_data"))
    monkeypatch.setattr(earthaccess, "search_datasets", _StrictMock("search_datasets"))

    target = resolve_target(tmp_db_path, "G-DB", "direct")
    assert target.kind == "granule"
    assert target.granule_concept_id == "G-DB"
    assert target.collection_concept_id == "C-DB"
    assert target.data_url == "s3://bucket/path/file.nc"
    assert target.provider == "PODAAC"
    assert target.sniffed_family == FormatFamily.NETCDF4
    assert target.source == "db"


def test_resolve_target_granule_db_miss(tmp_db_path: Path, monkeypatch) -> None:
    """Granule absent from DB → one search_data call, no search_datasets."""
    from nasa_virtual_zarr_survey.db import connect, init_schema

    # Empty DB (just the schema).
    con = connect(tmp_db_path)
    init_schema(con)
    con.close()

    import earthaccess

    g = _FakeGranule(
        concept_id="G-CMR",
        provider="GES_DISC",
        direct_url="s3://gesdisc/x/y.h5",
        external_url="https://gesdisc/x/y.h5",
        declared_format="HDF5",
    )
    calls = {"search_data": 0}

    def fake_search_data(*, concept_id: str, count: int = 1) -> list[_FakeGranule]:
        calls["search_data"] += 1
        return [g]

    monkeypatch.setattr(earthaccess, "search_data", fake_search_data)
    monkeypatch.setattr(earthaccess, "search_datasets", _StrictMock("search_datasets"))

    target = resolve_target(tmp_db_path, "G-CMR", "direct")
    assert target.source == "cmr"
    assert target.data_url == "s3://gesdisc/x/y.h5"
    assert target.provider == "GES_DISC"
    assert target.sniffed_family == FormatFamily.HDF5
    assert calls["search_data"] == 1


def test_resolve_target_granule_cmr_no_links(tmp_db_path: Path, monkeypatch) -> None:
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(tmp_db_path)
    init_schema(con)
    con.close()

    import earthaccess

    g = _FakeGranule(
        concept_id="G-CMR",
        provider="P",
        direct_url="",
        external_url="https://archive/x.nc",
        declared_format=None,
    )
    monkeypatch.setattr(
        earthaccess, "search_data", lambda **kw: [g] if kw.get("concept_id") else []
    )
    monkeypatch.setattr(earthaccess, "search_datasets", _StrictMock("search_datasets"))

    with pytest.raises(click.UsageError) as exc:
        resolve_target(tmp_db_path, "G-CMR", "direct")
    assert "no direct data link" in str(exc.value)
    assert "--access external" in str(exc.value)


def test_resolve_target_granule_not_found_anywhere(
    tmp_db_path: Path, monkeypatch
) -> None:
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(tmp_db_path)
    init_schema(con)
    con.close()

    import earthaccess

    monkeypatch.setattr(earthaccess, "search_data", lambda **kw: [])
    monkeypatch.setattr(earthaccess, "search_datasets", _StrictMock("search_datasets"))

    with pytest.raises(click.UsageError) as exc:
        resolve_target(tmp_db_path, "G-MISSING", "direct")
    assert "not found in survey DB or CMR" in str(exc.value)


# ---------------------------------------------------------------------------
# resolve_target — collection input
# ---------------------------------------------------------------------------


def test_resolve_target_collection_db_with_granules(
    tmp_db_path: Path, monkeypatch
) -> None:
    """DB has both collection row and a sampled granule → zero CMR calls."""
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from tests.conftest import insert_collection, insert_granule

    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(
        con,
        "C-FULL",
        short_name="n",
        daac="NSIDC",
        format_family="HDF5",
        format_declared="HDF5",
        processing_level="L2",
    )
    now = datetime.now(timezone.utc)
    insert_granule(
        con,
        "C-FULL",
        "G-FULL-1",
        data_url="s3://b/file1.h5",
        temporal_bin=1,
        sampled_at=now,
    )
    insert_granule(
        con,
        "C-FULL",
        "G-FULL-0",
        data_url="s3://b/file0.h5",
        temporal_bin=0,
        sampled_at=now,
    )
    con.close()

    import earthaccess

    monkeypatch.setattr(earthaccess, "search_data", _StrictMock("search_data"))
    monkeypatch.setattr(earthaccess, "search_datasets", _StrictMock("search_datasets"))

    target = resolve_target(tmp_db_path, "C-FULL", "direct")
    assert target.source == "db"
    assert target.kind == "collection"
    # Lowest temporal_bin first.
    assert target.granule_concept_id == "G-FULL-0"
    assert target.data_url == "s3://b/file0.h5"
    assert target.sniffed_family == FormatFamily.HDF5


def test_resolve_target_collection_db_no_granules(
    tmp_db_path: Path, monkeypatch
) -> None:
    """DB has the collection but no granules → one search_data call only."""
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from tests.conftest import insert_collection

    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(
        con,
        "C-SKIPPED",
        short_name="n",
        daac="ASF",
        format_family=None,
        format_declared="mystery format",
        processing_level="L1",
        skip_reason="format_unknown",
    )
    con.close()

    import earthaccess

    g = _FakeGranule(
        concept_id="G-CMR",
        provider="ASF",
        direct_url="s3://asf/foo.nc",
        external_url=None,
        declared_format="NetCDF-4",
    )
    calls = {"search_data": 0}

    def fake_search_data(*, concept_id: str, count: int = 1) -> list[_FakeGranule]:
        calls["search_data"] += 1
        return [g]

    monkeypatch.setattr(earthaccess, "search_data", fake_search_data)
    monkeypatch.setattr(earthaccess, "search_datasets", _StrictMock("search_datasets"))

    target = resolve_target(tmp_db_path, "C-SKIPPED", "direct")
    assert target.source == "cmr"
    assert target.granule_concept_id == "G-CMR"
    assert target.data_url == "s3://asf/foo.nc"
    assert target.daac == "ASF"  # from DB
    assert target.sniffed_family == FormatFamily.NETCDF4
    assert calls["search_data"] == 1


def test_resolve_target_collection_no_db_row(tmp_db_path: Path, monkeypatch) -> None:
    """No DB row → search_datasets + search_data (two CMR calls)."""
    from nasa_virtual_zarr_survey.db import connect, init_schema

    con = connect(tmp_db_path)
    init_schema(con)
    con.close()

    import earthaccess

    g = _FakeGranule(
        concept_id="G-CMR",
        provider="GES_DISC",
        direct_url="s3://gesdisc/x.h5",
        external_url=None,
        declared_format="HDF5",
    )
    calls = {"search_data": 0, "search_datasets": 0}

    def fake_search_data(*, concept_id: str, count: int = 1) -> list[_FakeGranule]:
        calls["search_data"] += 1
        return [g]

    def fake_search_datasets(*, concept_id: str, count: int = 1) -> list[Any]:
        calls["search_datasets"] += 1
        return [SimpleNamespace(concept_id=concept_id)]

    monkeypatch.setattr(earthaccess, "search_data", fake_search_data)
    monkeypatch.setattr(earthaccess, "search_datasets", fake_search_datasets)

    target = resolve_target(tmp_db_path, "C-NEW", "direct")
    assert target.source == "cmr"
    assert calls["search_datasets"] == 1
    assert calls["search_data"] == 1
    assert target.provider == "GES_DISC"


# ---------------------------------------------------------------------------
# generate_script
# ---------------------------------------------------------------------------


def _target_collection(
    *, family: FormatFamily | None = FormatFamily.NETCDF4
) -> ProbeTarget:
    return ProbeTarget(
        kind="collection",
        collection_concept_id="C123-DAAC",
        granule_concept_id="G456-DAAC",
        data_url="s3://bucket/path/file.nc",
        provider="POCLOUD",
        sniffed_family=family,
        daac="POCLOUD",
        source="db",
    )


def _target_granule(*, url: str = "https://archive.example/x.nc") -> ProbeTarget:
    return ProbeTarget(
        kind="granule",
        collection_concept_id="C-X",
        granule_concept_id="G456-DAAC",
        data_url=url,
        provider=None,
        sniffed_family=FormatFamily.NETCDF4,
        daac="DAAC",
        source="cmr",
    )


def test_generate_script_collection_sniffed_compiles() -> None:
    script = generate_script(_target_collection())
    _assert_compiles(script)
    # Section markers in the documented order.
    markers = [
        "# --- imports ---",
        "# --- argparse ---",
        "# --- earthaccess login ---",
        "# --- collection UMM ---",
        "# --- granule UMM ---",
        "# --- store construction ---",
        "# --- cache wiring (optional) ---",
        "# --- inspect ---",
    ]
    positions = [script.index(m) for m in markers]
    assert positions == sorted(positions)


def test_generate_script_unsniffed_format_skips_inspect() -> None:
    script = generate_script(_target_collection(family=None))
    _assert_compiles(script)
    assert "# format unknown" in script
    assert "# inspect_url(" in script
    # The actual call line is commented out.
    inspect_lines = [line for line in script.splitlines() if "inspect_url(" in line]
    assert all(line.lstrip().startswith("#") for line in inspect_lines)


def test_generate_script_granule_input_omits_collection_block() -> None:
    script = generate_script(_target_granule())
    _assert_compiles(script)
    assert "# --- collection UMM ---" not in script
    assert "# --- granule UMM ---" in script


def test_generate_script_s3_imports_s3store() -> None:
    script = generate_script(_target_collection())
    assert "from obstore.store import S3Store" in script
    assert "HTTPStore" not in script


def test_generate_script_https_imports_httpstore() -> None:
    script = generate_script(_target_granule(url="https://archive.example/x.nc"))
    assert "from obstore.store import HTTPStore" in script
    assert "HTTPStore.from_url" in script
    assert "S3Store" not in script
