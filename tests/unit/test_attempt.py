from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock


from nasa_virtual_zarr_survey.attempt import (
    AttemptResult,
    attempt_one,
    dispatch_parser,
)
from nasa_virtual_zarr_survey.formats import FormatFamily


def test_dispatch_parser_maps_known_families():
    p = dispatch_parser(FormatFamily.NETCDF4)
    assert p is not None
    # The HDFParser class comes from virtualizarr.parsers.hdf
    assert type(p).__name__ == "HDFParser"

    assert dispatch_parser(FormatFamily.HDF5) is not None
    assert dispatch_parser(FormatFamily.NETCDF3) is not None
    assert dispatch_parser(FormatFamily.DMRPP) is not None
    assert dispatch_parser(FormatFamily.FITS) is not None
    assert dispatch_parser(FormatFamily.ZARR) is not None


def test_dispatch_parser_returns_none_for_unsupported():
    assert dispatch_parser(FormatFamily.HDF4) is None
    assert dispatch_parser(FormatFamily.GEOTIFF) is None


def test_attempt_one_records_no_parser():
    result = attempt_one(
        url="s3://bucket/file.hdf",
        family=FormatFamily.HDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is False
    assert result.error_type == "NoParserAvailable"
    assert result.parser is None
    assert result.timed_out is False


def test_attempt_one_success(monkeypatch):
    def fake_ovd(url, registry, parser, **kwargs):
        assert url == "s3://bucket/file.nc"
        return MagicMock(name="Dataset")

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.open_virtual_dataset", fake_ovd
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is True
    assert result.error_type is None
    assert result.parser == "HDFParser"
    assert result.duration_s >= 0


def test_attempt_one_captures_exception(monkeypatch):
    def fake_ovd(*_, **__):
        raise ValueError("boom")

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.open_virtual_dataset", fake_ovd
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.success is False
    assert result.error_type == "ValueError"
    assert "boom" in result.error_message
    assert result.error_traceback is not None


def test_attempt_one_timeout(monkeypatch):
    import time

    def fake_ovd(*_, **__):
        time.sleep(10)

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.open_virtual_dataset", fake_ovd
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt._build_registry", lambda store, url: object()
    )

    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=1,
    )
    assert result.success is False
    assert result.timed_out is True
    assert result.error_type == "TimeoutError"


from pathlib import Path

from nasa_virtual_zarr_survey.attempt import ResultWriter, run_attempt
from nasa_virtual_zarr_survey.db import connect, init_schema


def test_result_writer_rotates_shards(tmp_results_dir: Path):
    w = ResultWriter(tmp_results_dir, shard_size=2)
    for i in range(5):
        w.append(AttemptResult(
            collection_concept_id="C1",
            granule_concept_id=f"G{i}",
            daac="PODAAC",
            format_family="NetCDF4",
            parser="HDFParser",
            success=True,
            duration_s=0.1,
            attempted_at=datetime.now(timezone.utc),
            stratified=True,
        ))
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

    # Pretend G1 is already attempted
    shard_dir = tmp_results_dir / "DAAC=PODAAC"
    shard_dir.mkdir(parents=True)
    import pyarrow as pa
    import pyarrow.parquet as pq
    already = pa.table({
        "collection_concept_id": ["C1"], "granule_concept_id": ["G1"],
        "daac": ["PODAAC"], "format_family": ["NetCDF4"], "parser": ["HDFParser"],
        "success": [True], "error_type": [None], "error_message": [None],
        "error_traceback": [None], "duration_s": [0.1], "timed_out": [False],
        "attempted_at": [datetime.now(timezone.utc)], "stratified": [True],
    })
    pq.write_table(already, shard_dir / "part-0000.parquet")

    attempts = []
    def fake_attempt_one(**kwargs):
        attempts.append(kwargs["granule_concept_id"])
        return AttemptResult(
            collection_concept_id=kwargs["collection_concept_id"],
            granule_concept_id=kwargs["granule_concept_id"],
            daac=kwargs["daac"], format_family=kwargs["family"].value,
            parser="HDFParser", success=True, duration_s=0.1,
            attempted_at=datetime.now(timezone.utc),
        )
    monkeypatch.setattr("nasa_virtual_zarr_survey.attempt.attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    n = run_attempt(tmp_db_path, tmp_results_dir, timeout_s=5, shard_size=500)
    assert n == 1
    assert attempts == ["G2"]
