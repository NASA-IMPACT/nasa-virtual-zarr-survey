from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from nasa_virtual_zarr_survey.attempt import (
    AttemptResult,
    attempt_one,
    dispatch_parser,
    ResultWriter,
    run_attempt,
)
from nasa_virtual_zarr_survey.db_session import SurveySession
from nasa_virtual_zarr_survey.formats import FormatFamily
from pathlib import Path

from nasa_virtual_zarr_survey.db import connect, init_schema
from tests.conftest import insert_collection, insert_granule


def _make_attempt_result(**overrides) -> AttemptResult:
    """Helper: build an AttemptResult with sensible defaults, accepting field overrides."""
    defaults = dict(
        collection_concept_id="C1",
        granule_concept_id="G1",
        daac="PODAAC",
        format_family="NetCDF4",
        parser="HDFParser",
        stratified=True,
        attempted_at=datetime.now(timezone.utc),
        parse_success=True,
        dataset_success=True,
        datatree_success=True,
        success=True,
        duration_s=0.1,
    )
    defaults.update(overrides)
    return AttemptResult(**defaults)


def test_dispatch_parser_maps_known_families():
    p = dispatch_parser(FormatFamily.NETCDF4)
    assert p is not None
    assert type(p).__name__ == "HDFParser"

    assert dispatch_parser(FormatFamily.HDF5) is not None
    assert dispatch_parser(FormatFamily.NETCDF3) is not None
    assert dispatch_parser(FormatFamily.DMRPP) is not None
    assert dispatch_parser(FormatFamily.FITS) is not None
    assert dispatch_parser(FormatFamily.ZARR) is not None

    geotiff_parser = dispatch_parser(FormatFamily.GEOTIFF)
    assert geotiff_parser is not None
    assert type(geotiff_parser).__name__ == "VirtualTIFF"


def test_dispatch_parser_returns_none_for_unsupported():
    assert dispatch_parser(FormatFamily.HDF4) is None


def test_attempt_one_records_no_parser():
    result = attempt_one(
        url="s3://bucket/file.hdf",
        family=FormatFamily.HDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is False
    assert result.parse_success is False
    assert result.dataset_success is None
    assert result.parse_error_type == "NoParserAvailable"
    assert result.parser is None
    assert result.timed_out is False


def test_attempt_one_success(monkeypatch):
    """All phases succeed: parser returns a manifest store, to_virtual_dataset and to_virtual_datatree succeed."""
    fake_ds = MagicMock(name="Dataset")
    fake_dt = MagicMock(name="DataTree")
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.return_value = fake_ds
    fake_ms.to_virtual_datatree.return_value = fake_dt

    def fake_parser_call(url, registry):
        assert url == "s3://bucket/file.nc"
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    # Patch dispatch_parser to return a callable mock
    fake_parser = MagicMock(side_effect=fake_parser_call)
    fake_parser.__class__.__name__ = "HDFParser"
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.parse_success is True
    assert result.dataset_success is True
    assert result.datatree_success is True
    assert result.success is True
    assert result.parse_error_type is None
    assert result.dataset_error_type is None
    assert result.datatree_error_type is None
    assert result.duration_s >= 0


def test_attempt_one_captures_parse_exception(monkeypatch):
    """Parser raises: parse_success=False, dataset_success=None, datatree_success=None."""

    def fake_parser_call(url, registry):
        raise ValueError("parser boom")

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is False
    assert result.parse_success is False
    assert result.dataset_success is None
    assert result.datatree_success is None
    assert result.parse_error_type == "ValueError"
    assert "parser boom" in result.parse_error_message
    assert result.parse_error_traceback is not None
    assert result.dataset_error_type is None
    assert result.datatree_error_type is None


def test_attempt_one_captures_dataset_exception(monkeypatch):
    """Parser succeeds but to_virtual_dataset raises: parse_success=True, dataset_success=False.
    datatree is still attempted and may succeed independently."""
    fake_dt = MagicMock(name="DataTree")
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.side_effect = RuntimeError("dataset boom")
    fake_ms.to_virtual_datatree.return_value = fake_dt

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    # success=True because parse succeeded AND datatree succeeded
    assert result.success is True
    assert result.parse_success is True
    assert result.dataset_success is False
    assert result.datatree_success is True
    assert result.parse_error_type is None
    assert result.dataset_error_type == "RuntimeError"
    assert "dataset boom" in result.dataset_error_message
    assert result.dataset_error_traceback is not None
    assert result.datatree_error_type is None


def test_attempt_one_dataset_fail_datatree_fail(monkeypatch):
    """Both 4a and 4b fail: success=False."""
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.side_effect = RuntimeError("dataset boom")
    fake_ms.to_virtual_datatree.side_effect = RuntimeError("datatree boom")

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is False
    assert result.parse_success is True
    assert result.dataset_success is False
    assert result.datatree_success is False
    assert result.dataset_error_type == "RuntimeError"
    assert result.datatree_error_type == "RuntimeError"


def test_attempt_one_dataset_fail_datatree_success(monkeypatch):
    """Parse succeeds, dataset fails, datatree succeeds: success=True, no fingerprint."""
    fake_dt = MagicMock(name="DataTree")
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.side_effect = ValueError("CONFLICTING_DIM_SIZES")
    fake_ms.to_virtual_datatree.return_value = fake_dt

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.parse_success is True
    assert result.dataset_success is False
    assert result.dataset_error_type == "ValueError"
    assert result.datatree_success is True
    assert result.datatree_error_type is None
    assert result.success is True
    # No fingerprint when only datatree succeeded
    assert result.fingerprint is None


def test_attempt_one_timeout_during_parse(monkeypatch):
    """Timeout during parse phase: timed_out_phase='parse'."""
    import time

    def fake_parser_call(url, registry):
        time.sleep(10)
        return MagicMock()

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=1,
    )
    assert result.success is False
    assert result.timed_out is True
    assert result.timed_out_phase == "parse"
    assert result.parse_error_type == "TimeoutError"
    assert result.parse_success is False
    assert result.dataset_success is None
    assert result.datatree_success is None


def test_attempt_one_timeout_during_dataset(monkeypatch):
    """Timeout during dataset phase: parse succeeds, timed_out_phase='dataset'."""
    import time

    fake_ms = MagicMock(name="ManifestStore")

    def slow_to_virtual_dataset():
        time.sleep(10)
        return MagicMock()

    fake_ms.to_virtual_dataset.side_effect = slow_to_virtual_dataset
    fake_ms.to_virtual_datatree.return_value = MagicMock()

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=1,
    )
    assert result.success is False
    assert result.timed_out is True
    assert result.timed_out_phase == "dataset"
    # parse succeeded (set inside worker before timeout)
    assert result.parse_success is True
    assert result.dataset_success is False
    assert result.dataset_error_type == "TimeoutError"


def test_attempt_one_timeout_during_datatree(monkeypatch):
    """Timeout during datatree phase: parse and dataset succeed, timed_out_phase='datatree'."""
    import time

    fake_ds = MagicMock(name="Dataset")
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.return_value = fake_ds

    def slow_to_virtual_datatree():
        time.sleep(10)
        return MagicMock()

    fake_ms.to_virtual_datatree.side_effect = slow_to_virtual_datatree

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=1,
    )
    assert result.timed_out is True
    assert result.timed_out_phase == "datatree"
    assert result.parse_success is True
    assert result.dataset_success is True
    assert result.datatree_success is False
    assert result.datatree_error_type == "TimeoutError"
    # success=True because dataset succeeded
    assert result.success is True


def test_result_writer_rotates_shards(tmp_results_dir: Path):
    w = ResultWriter(tmp_results_dir, shard_size=2)
    for i in range(5):
        w.append(_make_attempt_result(granule_concept_id=f"G{i}"))
    w.close()
    shards = sorted((tmp_results_dir / "DAAC=PODAAC").glob("*.parquet"))
    assert len(shards) >= 3


def test_run_attempt_resumes(tmp_db_path: Path, tmp_results_dir: Path, monkeypatch):
    con = connect(tmp_db_path)
    init_schema(con)
    # 2 granules in the DB
    insert_collection(con, "C1", num_granules=2)
    insert_granule(con, "C1", "G1", data_url="s3://b/a.nc", size_bytes=100)
    insert_granule(
        con, "C1", "G2", data_url="s3://b/b.nc", temporal_bin=1, size_bytes=100
    )
    con.close()

    # Pretend G1 is already attempted using the new schema
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    shard_dir = tmp_results_dir / "DAAC=PODAAC"
    shard_dir.mkdir(parents=True)
    cols = {f.name: [] for f in _SCHEMA}
    cols["collection_concept_id"].append("C1")
    cols["granule_concept_id"].append("G1")
    cols["daac"].append("PODAAC")
    cols["format_family"].append("NetCDF4")
    cols["parser"].append("HDFParser")
    cols["stratified"].append(True)
    cols["attempted_at"].append(datetime.now(timezone.utc))
    cols["parse_success"].append(True)
    cols["parse_error_type"].append(None)
    cols["parse_error_message"].append(None)
    cols["parse_error_traceback"].append(None)
    cols["parse_duration_s"].append(0.1)
    cols["dataset_success"].append(True)
    cols["dataset_error_type"].append(None)
    cols["dataset_error_message"].append(None)
    cols["dataset_error_traceback"].append(None)
    cols["dataset_duration_s"].append(0.1)
    cols["datatree_success"].append(False)
    cols["datatree_error_type"].append(None)
    cols["datatree_error_message"].append(None)
    cols["datatree_error_traceback"].append(None)
    cols["datatree_duration_s"].append(0.0)
    cols["success"].append(True)
    cols["override_applied"].append(False)
    cols["timed_out"].append(False)
    cols["timed_out_phase"].append(None)
    cols["duration_s"].append(0.2)
    cols["fingerprint"].append(None)
    pq.write_table(pa.table(cols, schema=_SCHEMA), shard_dir / "part-0000.parquet")

    attempts = []

    def fake_attempt_one(**kwargs):
        attempts.append(kwargs["granule_concept_id"])
        return _make_attempt_result(
            collection_concept_id=kwargs["collection_concept_id"],
            granule_concept_id=kwargs["granule_concept_id"],
            daac=kwargs["daac"],
            format_family=kwargs["family"].value,
        )

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.attempt_one", fake_attempt_one
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    n = run_attempt(
        SurveySession.from_duckdb(tmp_db_path),
        tmp_results_dir,
        timeout_s=5,
        shard_size=500,
    )
    assert n == 1
    assert attempts == ["G2"]


def test_run_attempt_aborts_on_consecutive_forbidden(
    tmp_db_path: Path, tmp_results_dir: Path, monkeypatch
):
    """Direct mode should abort after 5 consecutive 403 failures with an actionable error."""
    import pytest

    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1", num_granules=10)
    for i in range(10):
        insert_granule(
            con,
            "C1",
            f"G{i}",
            data_url=f"s3://b/f{i}.nc",
            temporal_bin=i,
            size_bytes=100,
        )
    con.close()

    def fake_attempt_one(**kwargs):
        return _make_attempt_result(
            collection_concept_id=kwargs["collection_concept_id"],
            granule_concept_id=kwargs["granule_concept_id"],
            daac=kwargs["daac"],
            format_family=kwargs["family"].value,
            parse_success=False,
            dataset_success=None,
            success=False,
            parse_error_type="ClientError",
            parse_error_message="403 Forbidden",
        )

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.attempt_one", fake_attempt_one
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    with pytest.raises(SystemExit) as exc_info:
        run_attempt(
            SurveySession.from_duckdb(tmp_db_path),
            tmp_results_dir,
            timeout_s=5,
            shard_size=500,
            access="direct",
        )

    assert "consecutive direct-S3 requests returned 403" in str(exc_info.value)
    assert "--access external" in str(exc_info.value)


def test_run_attempt_does_not_abort_on_mixed_failures(
    tmp_db_path: Path, tmp_results_dir: Path, monkeypatch
):
    """A single FORBIDDEN among other failures should not trigger the abort."""
    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1", num_granules=10)
    for i in range(10):
        insert_granule(
            con,
            "C1",
            f"G{i}",
            data_url=f"s3://b/f{i}.nc",
            temporal_bin=i,
            size_bytes=100,
        )
    con.close()

    call_count = {"n": 0}

    def fake_attempt_one(**kwargs):
        call_count["n"] += 1
        # Only every other call is FORBIDDEN
        if call_count["n"] % 2 == 0:
            return _make_attempt_result(
                collection_concept_id=kwargs["collection_concept_id"],
                granule_concept_id=kwargs["granule_concept_id"],
                daac=kwargs["daac"],
                format_family=kwargs["family"].value,
                parse_success=False,
                dataset_success=None,
                success=False,
                parse_error_type="ClientError",
                parse_error_message="403 Forbidden",
            )
        return _make_attempt_result(
            collection_concept_id=kwargs["collection_concept_id"],
            granule_concept_id=kwargs["granule_concept_id"],
            daac=kwargs["daac"],
            format_family=kwargs["family"].value,
            parse_success=False,
            dataset_success=None,
            success=False,
            parse_error_type="ValueError",
            parse_error_message="some other error",
        )

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.attempt_one", fake_attempt_one
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    n = run_attempt(
        SurveySession.from_duckdb(tmp_db_path),
        tmp_results_dir,
        timeout_s=5,
        shard_size=500,
        access="direct",
    )
    assert n == 10


def test_run_attempt_passes_cache_params_to_store_cache(tmp_path: Path):
    from nasa_virtual_zarr_survey.attempt import run_attempt
    from nasa_virtual_zarr_survey.db import connect, init_schema

    db_path = tmp_path / "survey.duckdb"
    results_dir = tmp_path / "results"
    con = connect(db_path)
    init_schema(con)
    con.close()

    cache_dir = tmp_path / "cache"
    captured = {}

    real_init = __import__(
        "nasa_virtual_zarr_survey.attempt", fromlist=["StoreCache"]
    ).StoreCache.__init__

    def spy_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        captured["args"] = args
        return real_init(self, *args, **kwargs)

    with patch("nasa_virtual_zarr_survey.attempt.StoreCache.__init__", spy_init):
        run_attempt(
            SurveySession.from_duckdb(db_path),
            results_dir,
            timeout_s=1,
            access="direct",
            cache_dir=cache_dir,
            cache_max_bytes=12345,
        )

    assert captured.get("cache_dir") == cache_dir
    assert captured.get("cache_max_bytes") == 12345


def test_run_attempt_no_overrides_uses_empty_registry(tmp_path: Path):
    """run_attempt(no_overrides=True) skips loading the override TOML entirely."""
    from nasa_virtual_zarr_survey.attempt import run_attempt

    db_path = tmp_path / "survey.duckdb"
    results_dir = tmp_path / "results"
    SurveySession.from_duckdb(db_path)

    overrides_path = tmp_path / "nonexistent.toml"
    n = run_attempt(
        SurveySession.from_duckdb(db_path),
        results_dir,
        timeout_s=1,
        access="direct",
        overrides_path=overrides_path,
        no_overrides=True,
    )
    assert n == 0


def test_run_attempt_skip_override_validation_does_not_validate(
    tmp_path: Path, monkeypatch
):
    """skip_override_validation=True: registry loaded but validate() never called."""
    from nasa_virtual_zarr_survey.attempt import run_attempt
    from nasa_virtual_zarr_survey.overrides import OverrideRegistry

    validate_calls: list[tuple] = []
    real_validate = OverrideRegistry.validate

    def spy_validate(self, *args, **kwargs):
        validate_calls.append((args, kwargs))
        return real_validate(self, *args, **kwargs)

    monkeypatch.setattr(OverrideRegistry, "validate", spy_validate)

    overrides = tmp_path / "overrides.toml"
    overrides.write_text("")

    db_path = tmp_path / "survey.duckdb"
    SurveySession.from_duckdb(db_path)

    run_attempt(
        SurveySession.from_duckdb(db_path),
        tmp_path / "results",
        timeout_s=1,
        access="direct",
        overrides_path=overrides,
        skip_override_validation=True,
    )
    assert validate_calls == []


def test_attempt_cli_locked_sample_runs(tmp_path: Path, monkeypatch) -> None:
    """`attempt --locked-sample PATH` constructs a session from JSON and runs."""
    import json

    from click.testing import CliRunner

    from nasa_virtual_zarr_survey.__main__ import cli

    sample = {
        "schema_version": 1,
        "created_at": "2026-04-26T12:00:00Z",
        "sampling_mode": "top=1",
        "collections": [
            {
                "concept_id": "C1-T",
                "daac": "X.DAAC",
                "format_family": "NetCDF4",
                "processing_level": "L4",
                "short_name": "FOO",
                "version": "1.0",
            }
        ],
        "granules": [
            {
                "collection_concept_id": "C1-T",
                "granule_concept_id": "G1-T",
                "s3_url": "s3://b/k1",
                "https_url": "https://h/k1",
                "temporal_bin": 0,
                "size_bytes": 100,
                "stratified": True,
            }
        ],
    }
    sample_path = tmp_path / "locked.json"
    sample_path.write_text(json.dumps(sample))
    results_dir = tmp_path / "results"

    import nasa_virtual_zarr_survey.attempt as attempt_mod

    def fake_attempt_one(**kwargs):
        return AttemptResult(
            collection_concept_id="C1-T",
            granule_concept_id="G1-T",
            daac="X.DAAC",
            format_family="NETCDF4",
            parser="HDFParser",
            parse_success=True,
            dataset_success=True,
            datatree_success=False,
            success=True,
            duration_s=0.1,
            attempted_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr(attempt_mod, "attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        attempt_mod.StoreCache,
        "get_store",
        lambda self, *, provider, url: object(),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "attempt",
            "--locked-sample",
            str(sample_path),
            "--results",
            str(results_dir),
            "--access",
            "direct",
            "--no-overrides",
        ],
    )
    assert result.exit_code == 0, result.output
    parquets = list(results_dir.glob("**/*.parquet"))
    assert parquets, f"expected at least one Parquet shard: {result.output}"


# ---------------------------------------------------------------------------
# Override integration
# ---------------------------------------------------------------------------


def test_attempt_one_threads_override_through(monkeypatch) -> None:
    from nasa_virtual_zarr_survey import attempt as attempt_mod
    from nasa_virtual_zarr_survey.overrides import CollectionOverride

    captured: dict = {}

    class FakeParser:
        def __init__(
            self,
            group: str | None = None,
            drop_variables: list[str] | None = None,
        ) -> None:
            captured["init"] = {
                "group": group,
                "drop_variables": drop_variables,
            }

        def __call__(self, *, url: str, registry):
            return FakeManifest()

    class FakeManifest:
        def to_virtual_dataset(self, **kw):
            captured["dataset"] = kw
            return object()

        def to_virtual_datatree(self, **kw):
            captured["datatree"] = kw
            return object()

    monkeypatch.setattr(
        attempt_mod,
        "dispatch_parser",
        lambda fam, kwargs=None: FakeParser(**(kwargs or {})),
    )
    monkeypatch.setattr(attempt_mod, "_build_registry", lambda store, url: object())

    override = CollectionOverride(
        parser_kwargs={"group": "science"},
        dataset_kwargs={"loadable_variables": []},
        datatree_kwargs={"loadable_variables": []},
        notes="test",
    )
    result = attempt_one(
        url="s3://bucket/key",
        family=FormatFamily.HDF5,
        store=object(),
        timeout_s=10,
        override=override,
    )
    assert captured["init"] == {"group": "science", "drop_variables": None}
    assert captured["dataset"] == {"loadable_variables": []}
    assert captured["datatree"] == {"loadable_variables": []}
    assert result.parse_success
    assert result.dataset_success is True
    assert result.datatree_success is True
    assert result.override_applied is True


def test_attempt_one_skip_dataset_skips_phase(monkeypatch) -> None:
    from nasa_virtual_zarr_survey import attempt as attempt_mod
    from nasa_virtual_zarr_survey.overrides import CollectionOverride

    class FakeParser:
        def __init__(self, **kw) -> None:
            pass

        def __call__(self, *, url: str, registry):
            return FakeManifest()

    class FakeManifest:
        def to_virtual_dataset(self, **kw):
            raise AssertionError("should not be called when skip_dataset=True")

        def to_virtual_datatree(self, **kw):
            return object()

    monkeypatch.setattr(
        attempt_mod,
        "dispatch_parser",
        lambda fam, kwargs=None: FakeParser(**(kwargs or {})),
    )
    monkeypatch.setattr(attempt_mod, "_build_registry", lambda store, url: object())

    result = attempt_one(
        url="s3://bucket/key",
        family=FormatFamily.HDF5,
        store=object(),
        timeout_s=10,
        override=CollectionOverride(skip_dataset=True, notes="datatree-only"),
    )
    assert result.parse_success
    assert result.dataset_success is None
    assert result.datatree_success is True
    # success requires parse + (dataset OR datatree) — datatree alone counts.
    assert result.success is True
    assert result.override_applied is True


def test_attempt_result_serializes_override_applied(tmp_path) -> None:
    w = ResultWriter(tmp_path, shard_size=1)
    w.append(
        AttemptResult(
            daac="X",
            attempted_at=datetime.now(timezone.utc),
            override_applied=True,
        )
    )
    w.close()

    [path] = list(tmp_path.glob("**/*.parquet"))
    table = pq.read_table(path)
    assert "override_applied" in table.schema.names
    assert table.column("override_applied").to_pylist() == [True]


def test_pending_granules_filters_by_collection(tmp_path) -> None:
    from nasa_virtual_zarr_survey.attempt import _pending_granules

    con = connect(str(tmp_path / "db.duckdb"))
    init_schema(con)
    con.execute(
        "INSERT INTO collections (concept_id, daac, provider, format_family, "
        "skip_reason) VALUES "
        "(?, ?, ?, ?, NULL), (?, ?, ?, ?, NULL)",
        ["C1-X", "X", "P", "NetCDF4", "C2-Y", "Y", "P", "NetCDF4"],
    )
    con.execute(
        "INSERT INTO granules (collection_concept_id, granule_concept_id, "
        "data_url, temporal_bin, stratified, access_mode) VALUES "
        "(?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
        [
            "C1-X",
            "G1",
            "s3://a/1",
            0,
            True,
            "direct",
            "C2-Y",
            "G2",
            "s3://b/2",
            0,
            True,
            "direct",
        ],
    )
    rows = _pending_granules(
        con,
        results_dir=tmp_path / "results",
        only_daac=None,
        only_collection="C1-X",
    )
    assert [r["collection_concept_id"] for r in rows] == ["C1-X"]
