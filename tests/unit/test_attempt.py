from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from nasa_virtual_zarr_survey.attempt import (
    AttemptResult,
    attempt_one,
    dispatch_parser,
    ResultWriter,
    run_attempt,
)
from nasa_virtual_zarr_survey.formats import FormatFamily
from pathlib import Path

from nasa_virtual_zarr_survey.db import connect, init_schema


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
    """Both phases succeed: parser returns a manifest store, to_virtual_dataset returns a Dataset."""
    fake_ds = MagicMock(name="Dataset")
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.return_value = fake_ds

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
        lambda family: fake_parser,
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.parse_success is True
    assert result.dataset_success is True
    assert result.success is True
    assert result.parse_error_type is None
    assert result.dataset_error_type is None
    assert result.duration_s >= 0


def test_attempt_one_captures_parse_exception(monkeypatch):
    """Parser raises: parse_success=False, dataset_success=None."""
    def fake_parser_call(url, registry):
        raise ValueError("parser boom")

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family: fake_parser,
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
    assert result.parse_error_type == "ValueError"
    assert "parser boom" in result.parse_error_message
    assert result.parse_error_traceback is not None
    assert result.dataset_error_type is None


def test_attempt_one_captures_dataset_exception(monkeypatch):
    """Parser succeeds but to_virtual_dataset raises: parse_success=True, dataset_success=False."""
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.side_effect = RuntimeError("dataset boom")

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family: fake_parser,
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
    assert result.parse_error_type is None
    assert result.dataset_error_type == "RuntimeError"
    assert "dataset boom" in result.dataset_error_message
    assert result.dataset_error_traceback is not None


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
        lambda family: fake_parser,
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


def test_attempt_one_timeout_during_dataset(monkeypatch):
    """Timeout during dataset phase: parse succeeds, timed_out_phase='dataset'."""
    import time

    fake_ms = MagicMock(name="ManifestStore")

    def slow_to_virtual_dataset():
        time.sleep(10)
        return MagicMock()

    fake_ms.to_virtual_dataset.side_effect = slow_to_virtual_dataset

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.dispatch_parser",
        lambda family: fake_parser,
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
    con.execute("""
        INSERT INTO collections VALUES
        ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',2,
         NULL, NULL, 'L3', NULL, now())
    """)
    con.execute(
        "INSERT INTO granules VALUES ('C1','G1','s3://b/a.nc',0,100,now(),TRUE)"
    )
    con.execute(
        "INSERT INTO granules VALUES ('C1','G2','s3://b/b.nc',1,100,now(),TRUE)"
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
    cols["success"].append(True)
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

    monkeypatch.setattr("nasa_virtual_zarr_survey.attempt.attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    n = run_attempt(tmp_db_path, tmp_results_dir, timeout_s=5, shard_size=500)
    assert n == 1
    assert attempts == ["G2"]


def test_run_attempt_aborts_on_consecutive_forbidden(tmp_db_path: Path, tmp_results_dir: Path, monkeypatch):
    """Direct mode should abort after 5 consecutive 403 failures with an actionable error."""
    import pytest

    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',10,
         NULL, NULL, 'L3', NULL, now())
    """)
    for i in range(10):
        con.execute(
            f"INSERT INTO granules VALUES ('C1','G{i}','s3://b/f{i}.nc',{i},100,now(),TRUE)"
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

    monkeypatch.setattr("nasa_virtual_zarr_survey.attempt.attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    with pytest.raises(SystemExit) as exc_info:
        run_attempt(tmp_db_path, tmp_results_dir, timeout_s=5, shard_size=500, access="direct")

    assert "consecutive direct-S3 requests returned 403" in str(exc_info.value)
    assert "--access external" in str(exc_info.value)


def test_run_attempt_does_not_abort_on_mixed_failures(tmp_db_path: Path, tmp_results_dir: Path, monkeypatch):
    """A single FORBIDDEN among other failures should not trigger the abort."""
    con = connect(tmp_db_path)
    init_schema(con)
    con.execute("""
        INSERT INTO collections VALUES
        ('C1','s','1','PODAAC','PODAAC','NetCDF4','NetCDF-4',10,
         NULL, NULL, 'L3', NULL, now())
    """)
    for i in range(10):
        con.execute(
            f"INSERT INTO granules VALUES ('C1','G{i}','s3://b/f{i}.nc',{i},100,now(),TRUE)"
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
                parse_success=False, dataset_success=None, success=False,
                parse_error_type="ClientError",
                parse_error_message="403 Forbidden",
            )
        return _make_attempt_result(
            collection_concept_id=kwargs["collection_concept_id"],
            granule_concept_id=kwargs["granule_concept_id"],
            daac=kwargs["daac"],
            format_family=kwargs["family"].value,
            parse_success=False, dataset_success=None, success=False,
            parse_error_type="ValueError", parse_error_message="some other error",
        )

    monkeypatch.setattr("nasa_virtual_zarr_survey.attempt.attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    n = run_attempt(tmp_db_path, tmp_results_dir, timeout_s=5, shard_size=500, access="direct")
    assert n == 10
