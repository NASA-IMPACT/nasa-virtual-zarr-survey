"""Unit tests for nasa_virtual_zarr_survey.repro."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nasa_virtual_zarr_survey.repro import FailureRow, generate_script


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row(**overrides) -> FailureRow:
    defaults = dict(
        collection_concept_id="C1-PODAAC",
        granule_concept_id="G1-PODAAC",
        daac="PODAAC",
        provider="PODAAC",
        format_family="NetCDF4",
        parser="HDFParser",
        data_url="https://data.example.nasa.gov/path/file.nc",
        phase="parse",
        error_type="OSError",
        error_message="Unable to open file",
        error_traceback=None,
        bucket="CANT_OPEN_FILE",
    )
    defaults.update(overrides)
    return FailureRow(**defaults)


# ---------------------------------------------------------------------------
# generate_script tests
# ---------------------------------------------------------------------------


def test_generate_script_https_parse_phase():
    row = _row()
    script = generate_script(row)
    assert "HTTPStore.from_url" in script
    assert "https://data.example.nasa.gov" in script
    assert "HDFParser" in script
    assert "C1-PODAAC" in script
    assert "G1-PODAAC" in script
    assert "Bucket: CANT_OPEN_FILE" in script
    # Parse-phase reproducer should NOT call to_virtual_dataset
    assert "to_virtual_dataset" not in script
    # It should call the parser directly
    assert "parser(url=url, registry=registry)" in script


def test_generate_script_https_dataset_phase():
    row = _row(phase="dataset", error_type="ValueError", bucket="DECODE_ERROR")
    script = generate_script(row)
    assert "to_virtual_dataset" in script
    assert "Bucket: DECODE_ERROR" in script


def test_generate_script_s3():
    row = _row(data_url="s3://podaac-bucket/path/file.nc")
    script = generate_script(row)
    assert "S3Store" in script
    assert "bucket='podaac-bucket'" in script or 'bucket="podaac-bucket"' in script
    assert "access_key_id=creds" in script
    assert "HTTPStore" not in script


def test_generate_script_fits():
    row = _row(
        parser="FITSParser",
        format_family="FITS",
        data_url="s3://b/f.fits",
        bucket="OTHER",
    )
    script = generate_script(row)
    assert "from virtualizarr.parsers.fits import FITSParser" in script
    assert "FITSParser()" in script


def test_generate_script_virtual_tiff():
    row = _row(
        parser="VirtualTIFF",
        format_family="GeoTIFF",
        data_url="s3://b/f.tif",
        bucket="OTHER",
    )
    script = generate_script(row)
    assert "from virtual_tiff import VirtualTIFF" in script
    assert "VirtualTIFF()" in script


def test_generate_script_no_parser_available():
    row = _row(
        parser=None,
        bucket="NO_PARSER",
        error_type="NoParserAvailable",
        error_message="No VirtualiZarr parser registered for HDF4",
        format_family="HDF4",
    )
    script = generate_script(row)
    assert (
        "No parser is registered" in script or "no registered parser" in script.lower()
    )
    # Doesn't try to run a bogus parser
    assert "parser(url=url" not in script


def test_generate_script_no_parser_error_type():
    """error_type == 'NoParserAvailable' also triggers the no-parser path."""
    row = _row(
        parser="SomeUnknownParser",
        bucket="NO_PARSER",
        error_type="NoParserAvailable",
        error_message="No parser",
        format_family="HDF4",
    )
    script = generate_script(row)
    assert (
        "No parser is registered" in script or "no registered parser" in script.lower()
    )
    assert "parser(url=url" not in script


def test_generate_script_unknown_parser_emits_stub():
    row = _row(parser="ExoticParser", bucket="OTHER")
    script = generate_script(row)
    # Should emit a TODO stub, not crash
    assert "ExoticParser" in script
    assert "TODO" in script or "Unknown parser" in script


def test_generate_script_includes_traceback():
    tb = "Traceback (most recent call last):\n  File 'x.py', line 1\nOSError: bad\n"
    row = _row(error_traceback=tb)
    script = generate_script(row)
    assert "Traceback" in script


def test_generate_script_all_parsers():
    """All known parsers generate valid scripts."""
    parsers = [
        "HDFParser",
        "NetCDF3Parser",
        "FITSParser",
        "DMRPPParser",
        "ZarrParser",
        "VirtualTIFF",
    ]
    for parser_name in parsers:
        row = _row(parser=parser_name)
        script = generate_script(row)
        assert parser_name in script, f"{parser_name} missing from generated script"
        assert "parser(url=url, registry=registry)" in script


def test_generate_script_s3_credentials_block():
    row = _row(
        data_url="s3://mybucket/data/file.nc",
        provider="GES_DISC",
    )
    script = generate_script(row)
    assert "get_s3_credentials" in script
    assert "GES_DISC" in script
    assert "secretAccessKey" in script
    assert "sessionToken" in script


def test_generate_script_https_registry_key():
    row = _row(data_url="https://data.podaac.earthdata.nasa.gov/myfile.nc")
    script = generate_script(row)
    assert "https://data.podaac.earthdata.nasa.gov" in script


def test_generate_script_phase_labels():
    parse_row = _row(phase="parse")
    dataset_row = _row(phase="dataset")
    assert "Parsability (Phase 3)" in generate_script(parse_row)
    assert "Datasetability (Phase 4)" in generate_script(dataset_row)


# ---------------------------------------------------------------------------
# find_failures tests
# ---------------------------------------------------------------------------


def test_find_failures_by_collection(tmp_db_path: Path, tmp_results_dir: Path):
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C1', 'n', '1', 'PODAAC', 'PODAAC', 'NetCDF4', 'NetCDF-4', 3, NULL, NULL, 'L3', NULL, now())"
    )
    # Also insert granule rows so data_url can be joined.
    now = datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO granules VALUES ('C1', 'G0', 'https://ex/good.nc', 0, NULL, ?, true)",
        [now],
    )
    con.execute(
        "INSERT INTO granules VALUES ('C1', 'G1', 'https://ex/bad.nc', 1, NULL, ?, true)",
        [now],
    )
    con.close()

    # Build a Parquet shard with _SCHEMA.
    shard_dir = tmp_results_dir / "DAAC=PODAAC"
    shard_dir.mkdir(parents=True)
    cols = {f.name: [] for f in _SCHEMA}
    for i, (pass_, url) in enumerate(
        [(True, "https://ex/good.nc"), (False, "https://ex/bad.nc")]
    ):
        cols["collection_concept_id"].append("C1")
        cols["granule_concept_id"].append(f"G{i}")
        cols["daac"].append("PODAAC")
        cols["format_family"].append("NetCDF4")
        cols["parser"].append("HDFParser")
        cols["stratified"].append(True)
        cols["attempted_at"].append(now)
        cols["parse_success"].append(pass_)
        cols["parse_error_type"].append(None if pass_ else "OSError")
        cols["parse_error_message"].append(None if pass_ else "bad file")
        cols["parse_error_traceback"].append(None)
        cols["parse_duration_s"].append(0.1)
        cols["dataset_success"].append(pass_ if pass_ else None)
        cols["dataset_error_type"].append(None)
        cols["dataset_error_message"].append(None)
        cols["dataset_error_traceback"].append(None)
        cols["dataset_duration_s"].append(0.1)
        cols["success"].append(pass_)
        cols["timed_out"].append(False)
        cols["timed_out_phase"].append(None)
        cols["duration_s"].append(0.2)
        cols["fingerprint"].append(None)
    pq.write_table(pa.table(cols, schema=_SCHEMA), shard_dir / "part-0000.parquet")

    rows = find_failures(
        tmp_db_path, tmp_results_dir, collection_concept_id="C1", limit=1
    )
    assert len(rows) == 1
    assert rows[0].granule_concept_id == "G1"
    assert rows[0].phase == "parse"
    assert rows[0].error_type == "OSError"
    assert rows[0].data_url == "https://ex/bad.nc"


def test_find_failures_by_granule(tmp_db_path: Path, tmp_results_dir: Path):
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    now = datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C2', 'n', '1', 'NSIDC', 'NSIDC', 'HDF5', 'HDF5', 1, NULL, NULL, 'L2', NULL, now())"
    )
    con.execute(
        "INSERT INTO granules VALUES ('C2', 'G-target', 's3://bucket/file.h5', 0, NULL, ?, true)",
        [now],
    )
    con.close()

    shard_dir = tmp_results_dir / "DAAC=NSIDC"
    shard_dir.mkdir(parents=True)
    cols = {f.name: [] for f in _SCHEMA}
    cols["collection_concept_id"].append("C2")
    cols["granule_concept_id"].append("G-target")
    cols["daac"].append("NSIDC")
    cols["format_family"].append("HDF5")
    cols["parser"].append("HDFParser")
    cols["stratified"].append(True)
    cols["attempted_at"].append(now)
    cols["parse_success"].append(False)
    cols["parse_error_type"].append("NotImplementedError")
    cols["parse_error_message"].append("variable length chunks not supported")
    cols["parse_error_traceback"].append(None)
    cols["parse_duration_s"].append(0.5)
    cols["dataset_success"].append(None)
    cols["dataset_error_type"].append(None)
    cols["dataset_error_message"].append(None)
    cols["dataset_error_traceback"].append(None)
    cols["dataset_duration_s"].append(0.0)
    cols["success"].append(False)
    cols["timed_out"].append(False)
    cols["timed_out_phase"].append(None)
    cols["duration_s"].append(0.5)
    cols["fingerprint"].append(None)
    pq.write_table(pa.table(cols, schema=_SCHEMA), shard_dir / "part-0000.parquet")

    rows = find_failures(
        tmp_db_path, tmp_results_dir, granule_concept_id="G-target", limit=1
    )
    assert len(rows) == 1
    assert rows[0].granule_concept_id == "G-target"
    assert rows[0].bucket == "VARIABLE_CHUNKS"
    assert rows[0].data_url == "s3://bucket/file.h5"


def test_find_failures_by_bucket(tmp_db_path: Path, tmp_results_dir: Path):
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    now = datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C3', 'n', '1', 'GES_DISC', 'GES_DISC', 'NetCDF4', 'NetCDF-4', 2, NULL, NULL, 'L3', NULL, now())"
    )
    for i in range(2):
        con.execute(
            f"INSERT INTO granules VALUES ('C3', 'G3-{i}', 'https://ex/file{i}.nc', {i}, NULL, ?, true)",
            [now],
        )
    con.close()

    shard_dir = tmp_results_dir / "DAAC=GES_DISC"
    shard_dir.mkdir(parents=True)
    cols = {f.name: [] for f in _SCHEMA}
    for i, (etype, emsg) in enumerate(
        [
            ("NotImplementedError", "codec zstd not supported"),
            ("NotImplementedError", "codec bzip2 not supported"),
        ]
    ):
        cols["collection_concept_id"].append("C3")
        cols["granule_concept_id"].append(f"G3-{i}")
        cols["daac"].append("GES_DISC")
        cols["format_family"].append("NetCDF4")
        cols["parser"].append("HDFParser")
        cols["stratified"].append(True)
        cols["attempted_at"].append(now)
        cols["parse_success"].append(False)
        cols["parse_error_type"].append(etype)
        cols["parse_error_message"].append(emsg)
        cols["parse_error_traceback"].append(None)
        cols["parse_duration_s"].append(0.1)
        cols["dataset_success"].append(None)
        cols["dataset_error_type"].append(None)
        cols["dataset_error_message"].append(None)
        cols["dataset_error_traceback"].append(None)
        cols["dataset_duration_s"].append(0.0)
        cols["success"].append(False)
        cols["timed_out"].append(False)
        cols["timed_out_phase"].append(None)
        cols["duration_s"].append(0.1)
        cols["fingerprint"].append(None)
    pq.write_table(pa.table(cols, schema=_SCHEMA), shard_dir / "part-0000.parquet")

    rows = find_failures(
        tmp_db_path, tmp_results_dir, bucket="UNSUPPORTED_CODEC", limit=5
    )
    assert len(rows) == 2
    assert all(r.bucket == "UNSUPPORTED_CODEC" for r in rows)


def test_find_failures_no_results(tmp_db_path: Path, tmp_results_dir: Path):
    """Returns empty list when DB has no Parquet results yet."""
    from nasa_virtual_zarr_survey.repro import find_failures

    rows = find_failures(
        tmp_db_path, tmp_results_dir, collection_concept_id="C-NONEXISTENT", limit=1
    )
    assert rows == []


def test_find_failures_phase_filter(tmp_db_path: Path, tmp_results_dir: Path):
    """Phase filter excludes rows from the wrong phase."""
    from nasa_virtual_zarr_survey.db import connect, init_schema
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    now = datetime.now(timezone.utc)
    con.execute(
        "INSERT INTO collections VALUES "
        "('C4', 'n', '1', 'ORNL', 'ORNL', 'NetCDF4', 'NetCDF-4', 1, NULL, NULL, 'L4', NULL, now())"
    )
    con.execute(
        "INSERT INTO granules VALUES ('C4', 'G4', 'https://ex/f.nc', 0, NULL, ?, true)",
        [now],
    )
    con.close()

    shard_dir = tmp_results_dir / "DAAC=ORNL"
    shard_dir.mkdir(parents=True)
    cols = {f.name: [] for f in _SCHEMA}
    cols["collection_concept_id"].append("C4")
    cols["granule_concept_id"].append("G4")
    cols["daac"].append("ORNL")
    cols["format_family"].append("NetCDF4")
    cols["parser"].append("HDFParser")
    cols["stratified"].append(True)
    cols["attempted_at"].append(now)
    # Parse succeeds, dataset fails.
    cols["parse_success"].append(True)
    cols["parse_error_type"].append(None)
    cols["parse_error_message"].append(None)
    cols["parse_error_traceback"].append(None)
    cols["parse_duration_s"].append(0.1)
    cols["dataset_success"].append(False)
    cols["dataset_error_type"].append("ValueError")
    cols["dataset_error_message"].append("compound dtype not supported")
    cols["dataset_error_traceback"].append(None)
    cols["dataset_duration_s"].append(0.2)
    cols["success"].append(False)
    cols["timed_out"].append(False)
    cols["timed_out_phase"].append(None)
    cols["duration_s"].append(0.3)
    cols["fingerprint"].append(None)
    pq.write_table(pa.table(cols, schema=_SCHEMA), shard_dir / "part-0000.parquet")

    # Filtering for parse phase should find nothing.
    parse_rows = find_failures(
        tmp_db_path, tmp_results_dir, collection_concept_id="C4", phase="parse", limit=5
    )
    assert parse_rows == []

    # Filtering for dataset phase should find the row.
    dataset_rows = find_failures(
        tmp_db_path,
        tmp_results_dir,
        collection_concept_id="C4",
        phase="dataset",
        limit=5,
    )
    assert len(dataset_rows) == 1
    assert dataset_rows[0].phase == "dataset"
    assert dataset_rows[0].error_type == "ValueError"
