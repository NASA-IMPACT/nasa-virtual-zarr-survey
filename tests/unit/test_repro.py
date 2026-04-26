"""Unit tests for nasa_virtual_zarr_survey.repro."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nasa_virtual_zarr_survey.db import connect, init_schema
from nasa_virtual_zarr_survey.repro import FailureRow, generate_script
from tests.conftest import insert_collection, insert_granule


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
        https_url=None,
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


def test_generate_script_includes_https_download_url_when_distinct():
    """When data_url is s3://... and a separate HTTPS URL is captured, the
    docstring should expose the HTTPS form so a reader can fetch the granule
    with curl/wget."""
    row = _row(
        data_url="s3://podaac-bucket/path/file.nc",
        https_url="https://archive.podaac.earthdata.nasa.gov/path/file.nc",
    )
    script = generate_script(row)
    assert "URL: s3://podaac-bucket/path/file.nc" in script
    assert "Download URL" in script
    assert "https://archive.podaac.earthdata.nasa.gov/path/file.nc" in script


def test_generate_script_omits_redundant_download_url():
    """When https_url equals data_url (e.g. survey ran with --access external),
    the docstring should not duplicate the line."""
    url = "https://data.podaac.earthdata.nasa.gov/file.nc"
    row = _row(data_url=url, https_url=url)
    script = generate_script(row)
    assert script.count(url) >= 1
    assert "Download URL" not in script


def test_generated_script_accepts_cache_flags() -> None:
    from nasa_virtual_zarr_survey.repro import FailureRow, generate_script

    row = FailureRow(
        collection_concept_id="C123-DAAC",
        granule_concept_id="G456-DAAC",
        daac="DAAC",
        provider="POCLOUD",
        format_family="NETCDF4",
        parser="HDFParser",
        data_url="s3://bucket/path/file.nc",
        https_url=None,
        phase="parse",
        error_type="OSError",
        error_message="boom",
        error_traceback=None,
        bucket="CANT_OPEN_FILE",
    )
    script = generate_script(row)
    # Argparse for cache flags is present.
    assert "--cache" in script
    assert "--cache-dir" in script
    assert "--cache-max-size" in script
    # Wrapper import only present when cache flag triggers — but we always emit
    # the import so the script type-checks.
    assert "DiskCachingReadableStore" in script
    # Script compiles.
    import py_compile
    import tempfile
    from pathlib import Path as _P

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(script)
        path = _P(f.name)
    try:
        py_compile.compile(str(path), doraise=True)
    finally:
        path.unlink()


# ---------------------------------------------------------------------------
# find_failures tests
# ---------------------------------------------------------------------------


def test_find_failures_by_collection(tmp_db_path: Path, tmp_results_dir: Path):
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    insert_collection(con, "C1", short_name="n", num_granules=3)
    # Also insert granule rows so data_url can be joined.
    now = datetime.now(timezone.utc)
    insert_granule(
        con,
        "C1",
        "G0",
        data_url="https://ex/good.nc",
        sampled_at=now,
        access_mode="external",
    )
    insert_granule(
        con,
        "C1",
        "G1",
        data_url="https://ex/bad.nc",
        https_url="https://archive.example/bad.nc",
        temporal_bin=1,
        sampled_at=now,
        access_mode="external",
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
        cols["datatree_success"].append(None)
        cols["datatree_error_type"].append(None)
        cols["datatree_error_message"].append(None)
        cols["datatree_error_traceback"].append(None)
        cols["datatree_duration_s"].append(0.0)
        cols["success"].append(pass_)
        cols["override_applied"].append(False)
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
    assert rows[0].https_url == "https://archive.example/bad.nc"


def test_find_failures_by_granule(tmp_db_path: Path, tmp_results_dir: Path):
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    now = datetime.now(timezone.utc)
    insert_collection(
        con,
        "C2",
        short_name="n",
        daac="NSIDC",
        format_family="HDF5",
        format_declared="HDF5",
        processing_level="L2",
    )
    insert_granule(
        con,
        "C2",
        "G-target",
        data_url="s3://bucket/file.h5",
        https_url="https://archive.example/file.h5",
        sampled_at=now,
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
    cols["datatree_success"].append(None)
    cols["datatree_error_type"].append(None)
    cols["datatree_error_message"].append(None)
    cols["datatree_error_traceback"].append(None)
    cols["datatree_duration_s"].append(0.0)
    cols["success"].append(False)
    cols["override_applied"].append(False)
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
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    now = datetime.now(timezone.utc)
    insert_collection(
        con,
        "C3",
        short_name="n",
        daac="GES_DISC",
        num_granules=2,
    )
    for i in range(2):
        insert_granule(
            con,
            "C3",
            f"G3-{i}",
            data_url=f"https://ex/file{i}.nc",
            temporal_bin=i,
            sampled_at=now,
            access_mode="external",
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
        cols["datatree_success"].append(None)
        cols["datatree_error_type"].append(None)
        cols["datatree_error_message"].append(None)
        cols["datatree_error_traceback"].append(None)
        cols["datatree_duration_s"].append(0.0)
        cols["success"].append(False)
        cols["override_applied"].append(False)
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
    from nasa_virtual_zarr_survey.repro import find_failures
    from nasa_virtual_zarr_survey.attempt import _SCHEMA

    con = connect(tmp_db_path)
    init_schema(con)
    now = datetime.now(timezone.utc)
    insert_collection(
        con,
        "C4",
        short_name="n",
        daac="ORNL",
        processing_level="L4",
    )
    insert_granule(
        con,
        "C4",
        "G4",
        data_url="https://ex/f.nc",
        sampled_at=now,
        access_mode="external",
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
    cols["datatree_success"].append(None)
    cols["datatree_error_type"].append(None)
    cols["datatree_error_message"].append(None)
    cols["datatree_error_traceback"].append(None)
    cols["datatree_duration_s"].append(0.0)
    cols["success"].append(False)
    cols["override_applied"].append(False)
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


# ---------------------------------------------------------------------------
# Override + inspector integration
# ---------------------------------------------------------------------------


def test_generated_script_includes_override_kwargs() -> None:
    from nasa_virtual_zarr_survey.overrides import CollectionOverride

    row = _row(phase="dataset", parser="HDFParser", data_url="s3://bucket/key.nc")
    override = CollectionOverride(
        parser_kwargs={"group": "science"},
        dataset_kwargs={"loadable_variables": []},
        notes="t",
    )
    src = generate_script(row, override=override)
    assert "group='science'" in src
    assert "loadable_variables=[]" in src
    assert "to_virtual_dataset" in src


def test_generated_script_without_override_omits_kwargs() -> None:
    row = _row(phase="dataset")
    src = generate_script(row)
    # The dataset call still appears, just with empty parens.
    assert "to_virtual_dataset()" in src


def test_generated_script_has_cache_argparse_only() -> None:
    """``probe`` owns inspection now — ``repro`` no longer emits the
    ``--inspect``/``--attempt`` mutex or the ``inspect_url`` call."""
    row = _row()
    src = generate_script(row)
    assert "import argparse" in src
    assert "--cache" in src
    assert "--inspect" not in src
    assert "--attempt" not in src
    assert "if do_inspect" not in src
    assert "inspect_url(" not in src
    assert "from nasa_virtual_zarr_survey.inspect" not in src


def test_generated_script_docstring_points_at_probe() -> None:
    row = _row()
    src = generate_script(row)
    assert "nasa-virtual-zarr-survey probe" in src
    assert "starting point for non-debugging" in src


def test_generated_script_compiles() -> None:
    """A generated script must be syntactically valid Python."""
    src = generate_script(_row())
    compile(src, "<repro_test>", "exec")


def test_repro_cli_no_overrides_skips_kwargs(tmp_path, monkeypatch) -> None:
    from click.testing import CliRunner

    from nasa_virtual_zarr_survey.__main__ import cli
    from nasa_virtual_zarr_survey import repro as repro_mod

    row = _row(
        collection_concept_id="C1-POCLOUD",
        granule_concept_id="G1",
        format_family="NetCDF4",
        parser="HDFParser",
        data_url="s3://b/k",
    )
    monkeypatch.setattr(repro_mod, "find_failures", lambda *a, **k: [row])

    monkeypatch.chdir(tmp_path)
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "collection_overrides.toml").write_text(
        '[C1-POCLOUD]\nparser = { group = "science" }\nnotes = "t"\n'
    )

    out_dir = tmp_path / "out"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["repro", "C1-POCLOUD", "--no-overrides", "--out", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    [path] = list(out_dir.glob("*.py"))
    text = path.read_text()
    assert "group='science'" not in text


def test_repro_cli_default_bakes_overrides_in(tmp_path, monkeypatch) -> None:
    from click.testing import CliRunner

    from nasa_virtual_zarr_survey.__main__ import cli
    from nasa_virtual_zarr_survey import repro as repro_mod

    row = _row(
        collection_concept_id="C1-POCLOUD",
        granule_concept_id="G1",
        format_family="NetCDF4",
        parser="HDFParser",
        data_url="s3://b/k",
    )
    monkeypatch.setattr(repro_mod, "find_failures", lambda *a, **k: [row])

    monkeypatch.chdir(tmp_path)
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "collection_overrides.toml").write_text(
        '[C1-POCLOUD]\nparser = { group = "science" }\nnotes = "t"\n'
    )

    out_dir = tmp_path / "out"
    runner = CliRunner()
    result = runner.invoke(cli, ["repro", "C1-POCLOUD", "--out", str(out_dir)])
    assert result.exit_code == 0, result.output
    [path] = list(out_dir.glob("*.py"))
    assert "group='science'" in path.read_text()
