from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vzc.pipeline._attempt import (
    AttemptResult,
    attempt_one,
    dispatch_parser,
    ResultWriter,
    _run_attempt,
)
from vzc.core.formats import FormatFamily
from vzc.state._io import load_state, save_state
from tests.conftest import make_collection, make_granule, make_state


def _make_attempt_result(**overrides) -> AttemptResult:
    """Helper: build an AttemptResult with sensible defaults."""
    defaults = dict(
        collection_concept_id="C1",
        granule_concept_id="G1",
        daac="PODAAC",
        format_family="NetCDF4",
        parser="HDFParser",
        attempted_at=datetime.now(timezone.utc),
        parse_success=True,
        dataset_success=True,
        datatree_success=True,
        success=True,
        duration_s=0.1,
    )
    defaults.update(overrides)
    return AttemptResult(**defaults)


# ---------------------------------------------------------------------------
# attempt_one (no DB)
# ---------------------------------------------------------------------------


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
    fake_ds = MagicMock(name="Dataset")
    fake_dt = MagicMock(name="DataTree")
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.return_value = fake_ds
    fake_ms.to_virtual_datatree.return_value = fake_dt

    def fake_parser_call(url, registry):
        assert url == "s3://bucket/file.nc"
        return fake_ms

    monkeypatch.setattr(
        "vzc.pipeline._attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    fake_parser.__class__.__name__ = "HDFParser"
    monkeypatch.setattr(
        "vzc.pipeline._attempt.dispatch_parser",
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


def test_attempt_one_logs_when_fingerprint_extraction_fails(monkeypatch, caplog):
    fake_ds = MagicMock(name="Dataset")
    fake_dt = MagicMock(name="DataTree")
    fake_ms = MagicMock(name="ManifestStore")
    fake_ms.to_virtual_dataset.return_value = fake_ds
    fake_ms.to_virtual_datatree.return_value = fake_dt

    def fake_parser_call(url, registry):
        return fake_ms

    monkeypatch.setattr(
        "vzc.pipeline._attempt._build_registry", lambda store, url: object()
    )
    fake_parser = MagicMock(side_effect=fake_parser_call)
    fake_parser.__class__.__name__ = "HDFParser"
    monkeypatch.setattr(
        "vzc.pipeline._attempt.dispatch_parser",
        lambda family, **_: fake_parser,
    )

    def explode(*a, **kw):
        raise RuntimeError("fingerprint regression")

    monkeypatch.setattr("vzc.pipeline._cubability.extract_fingerprint", explode)

    import logging

    with caplog.at_level(logging.WARNING, logger="vzc.pipeline._attempt"):
        result = attempt_one(
            url="s3://bucket/file.nc",
            family=FormatFamily.NETCDF4,
            store=object(),
            timeout_s=5,
        )

    assert result.success is True
    assert any("fingerprint extraction failed" in r.message for r in caplog.records)


def test_attempt_one_captures_parse_exception(monkeypatch):
    class BoomParser:
        def __call__(self, *, url, registry):
            raise ValueError("invalid HDF5")

    monkeypatch.setattr(
        "vzc.pipeline._attempt.dispatch_parser",
        lambda fam, **_: BoomParser(),
    )
    monkeypatch.setattr("vzc.pipeline._attempt._build_registry", lambda s, u: object())
    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.parse_success is False
    assert result.parse_error_type == "ValueError"
    assert "invalid HDF5" in (result.parse_error_message or "")
    assert result.dataset_success is None


def test_attempt_one_captures_dataset_exception(monkeypatch):
    class FakeMS:
        def to_virtual_dataset(self, **kw):
            raise RuntimeError("ds boom")

        def to_virtual_datatree(self, **kw):
            return object()

    class FakeParser:
        def __call__(self, *, url, registry):
            return FakeMS()

    monkeypatch.setattr(
        "vzc.pipeline._attempt.dispatch_parser",
        lambda fam, **_: FakeParser(),
    )
    monkeypatch.setattr("vzc.pipeline._attempt._build_registry", lambda s, u: object())
    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.parse_success is True
    assert result.dataset_success is False
    assert result.dataset_error_type == "RuntimeError"
    assert result.datatree_success is True


def test_attempt_one_dataset_fail_datatree_fail(monkeypatch):
    class FakeMS:
        def to_virtual_dataset(self, **kw):
            raise RuntimeError("ds boom")

        def to_virtual_datatree(self, **kw):
            raise RuntimeError("dt boom")

    monkeypatch.setattr(
        "vzc.pipeline._attempt.dispatch_parser",
        lambda fam, **_: type(
            "P", (), {"__call__": lambda self, *, url, registry: FakeMS()}
        )(),
    )
    monkeypatch.setattr("vzc.pipeline._attempt._build_registry", lambda s, u: object())
    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.parse_success is True
    assert result.dataset_success is False
    assert result.datatree_success is False
    assert result.success is False


def test_attempt_one_dataset_fail_datatree_success(monkeypatch):
    class FakeMS:
        def to_virtual_dataset(self, **kw):
            raise RuntimeError("ds boom")

        def to_virtual_datatree(self, **kw):
            return object()

    monkeypatch.setattr(
        "vzc.pipeline._attempt.dispatch_parser",
        lambda fam, **_: type(
            "P", (), {"__call__": lambda self, *, url, registry: FakeMS()}
        )(),
    )
    monkeypatch.setattr("vzc.pipeline._attempt._build_registry", lambda s, u: object())
    result = attempt_one(
        url="s3://bucket/file.nc",
        family=FormatFamily.NETCDF4,
        store=object(),
        timeout_s=5,
    )
    assert result.parse_success is True
    assert result.dataset_success is False
    assert result.datatree_success is True
    assert result.success is True


# ---------------------------------------------------------------------------
# ResultWriter
# ---------------------------------------------------------------------------


def test_result_writer_rotates_shards(tmp_results_dir: Path):
    w = ResultWriter(tmp_results_dir, shard_size=2)
    for i in range(5):
        w.append(_make_attempt_result(granule_concept_id=f"G{i}"))
    w.close()
    shards = sorted((tmp_results_dir / "DAAC=PODAAC").glob("*.parquet"))
    assert len(shards) >= 3


def test_attempt_result_serializes_override_applied(tmp_path: Path) -> None:
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


# ---------------------------------------------------------------------------
# _run_attempt + _pending_attempts
# ---------------------------------------------------------------------------


def test_run_attempt_resumes(tmp_state_path: Path, tmp_results_dir: Path, monkeypatch):
    state = make_state(
        collections=[make_collection("C1", num_granules=2)],
        granules=[
            make_granule(
                "C1",
                "G1",
                s3_url="s3://b/a.nc",
                https_url="https://b/a.nc",
                size_bytes=100,
            ),
            make_granule(
                "C1",
                "G2",
                s3_url="s3://b/b.nc",
                https_url="https://b/b.nc",
                stratification_bin=1,
                size_bytes=100,
            ),
        ],
    )
    save_state(state, tmp_state_path)

    # Pretend G1 is already attempted using the new schema
    from vzc.pipeline._attempt import _SCHEMA

    shard_dir = tmp_results_dir / "DAAC=PODAAC"
    shard_dir.mkdir(parents=True)
    cols = {f.name: [] for f in _SCHEMA}
    cols["collection_concept_id"].append("C1")
    cols["granule_concept_id"].append("G1")
    cols["daac"].append("PODAAC")
    cols["format_family"].append("NetCDF4")
    cols["parser"].append("HDFParser")
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

    monkeypatch.setattr("vzc.pipeline._attempt.attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        "vzc.pipeline._attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    n = _run_attempt(
        load_state(tmp_state_path),
        access="direct",
        results_dir=tmp_results_dir,
        cache_dir=None,
        timeout_s=5,
        shard_size=500,
        skip_override_validation=True,
    )
    assert n == 1
    assert attempts == ["G2"]


def test_run_attempt_aborts_on_consecutive_forbidden(
    tmp_state_path: Path, tmp_results_dir: Path, monkeypatch
):
    """Direct mode should abort after 5 consecutive 403 failures."""
    state = make_state(
        collections=[make_collection("C1", num_granules=10)],
        granules=[
            make_granule(
                "C1",
                f"G{i}",
                s3_url=f"s3://b/f{i}.nc",
                https_url=f"https://b/f{i}.nc",
                stratification_bin=i,
                size_bytes=100,
            )
            for i in range(10)
        ],
    )
    save_state(state, tmp_state_path)

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

    monkeypatch.setattr("vzc.pipeline._attempt.attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        "vzc.pipeline._attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_attempt(
            load_state(tmp_state_path),
            access="direct",
            results_dir=tmp_results_dir,
            cache_dir=None,
            timeout_s=5,
            shard_size=500,
            skip_override_validation=True,
        )

    assert "consecutive direct-S3 requests returned 403" in str(exc_info.value)
    assert "--access external" in str(exc_info.value)


def test_run_attempt_does_not_abort_on_mixed_failures(
    tmp_state_path: Path, tmp_results_dir: Path, monkeypatch
):
    """A single FORBIDDEN among other failures should not trigger the abort."""
    state = make_state(
        collections=[make_collection("C1", num_granules=10)],
        granules=[
            make_granule(
                "C1",
                f"G{i}",
                s3_url=f"s3://b/f{i}.nc",
                https_url=f"https://b/f{i}.nc",
                stratification_bin=i,
                size_bytes=100,
            )
            for i in range(10)
        ],
    )
    save_state(state, tmp_state_path)

    call_count = {"n": 0}

    def fake_attempt_one(**kwargs):
        call_count["n"] += 1
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

    monkeypatch.setattr("vzc.pipeline._attempt.attempt_one", fake_attempt_one)
    monkeypatch.setattr(
        "vzc.pipeline._attempt.StoreCache.get_store",
        lambda self, *, provider, url: object(),
    )

    n = _run_attempt(
        load_state(tmp_state_path),
        access="direct",
        results_dir=tmp_results_dir,
        cache_dir=None,
        timeout_s=5,
        shard_size=500,
        skip_override_validation=True,
    )
    assert n == 10


def test_run_attempt_passes_cache_params_to_store_cache(tmp_path: Path):
    state_path = tmp_path / "state.json"
    save_state(make_state(), state_path)
    results_dir = tmp_path / "results"
    cache_dir = tmp_path / "cache"
    captured: dict = {}

    real_init = __import__(
        "vzc.pipeline._attempt", fromlist=["StoreCache"]
    ).StoreCache.__init__

    def spy_init(self, *args, **kwargs):
        captured.update(kwargs)
        captured["args"] = args
        return real_init(self, *args, **kwargs)

    with patch("vzc.pipeline._attempt.StoreCache.__init__", spy_init):
        _run_attempt(
            load_state(state_path),
            access="direct",
            results_dir=results_dir,
            cache_dir=cache_dir,
            timeout_s=1,
            skip_override_validation=True,
        )

    assert captured.get("access") == "direct"


def test_run_attempt_skip_override_validation_does_not_validate(
    tmp_path: Path, monkeypatch
):
    from vzc.pipeline._overrides import OverrideRegistry

    validate_calls: list[tuple] = []
    real_validate = OverrideRegistry.validate

    def spy_validate(self, *args, **kwargs):
        validate_calls.append((args, kwargs))
        return real_validate(self, *args, **kwargs)

    monkeypatch.setattr(OverrideRegistry, "validate", spy_validate)

    overrides = tmp_path / "overrides.toml"
    overrides.write_text("")

    state_path = tmp_path / "state.json"
    save_state(make_state(), state_path)

    _run_attempt(
        load_state(state_path),
        access="direct",
        results_dir=tmp_path / "results",
        cache_dir=None,
        timeout_s=1,
        overrides_path=overrides,
        skip_override_validation=True,
    )
    assert validate_calls == []


def test_attempt_cli_locked_sample_runs(tmp_path: Path, monkeypatch) -> None:
    """``attempt --locked-sample PATH`` constructs a session from JSON and runs."""
    import json

    from click.testing import CliRunner

    from vzc.__main__ import cli
    from vzc.state._io import SCHEMA_VERSION

    sample = {
        "schema_version": SCHEMA_VERSION,
        "run_meta": {"sampling_mode": "top=1"},
        "collections": [
            {
                "concept_id": "C1-T",
                "daac": "X.DAAC",
                "provider": "PODAAC",
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
                "stratification_bin": 0,
                "n_total_at_sample": 0,
                "size_bytes": 100,
            }
        ],
    }
    monkeypatch.chdir(tmp_path)
    state_path = tmp_path / "output" / "state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(sample))
    results_dir = tmp_path / "output" / "results"
    overrides_path = tmp_path / "config" / "collection_overrides.toml"
    overrides_path.parent.mkdir(parents=True, exist_ok=True)
    overrides_path.write_text("")

    import vzc.pipeline._attempt as attempt_mod

    def fake_attempt_one(**kwargs):
        return AttemptResult(
            collection_concept_id="C1-T",
            granule_concept_id="G1-T",
            daac="X.DAAC",
            format_family="NetCDF4",
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
    result = runner.invoke(cli, ["attempt", "--access", "direct"])
    assert result.exit_code == 0, result.output
    parquets = list(results_dir.glob("**/*.parquet"))
    assert parquets, f"expected at least one Parquet shard: {result.output}"


# ---------------------------------------------------------------------------
# Override integration
# ---------------------------------------------------------------------------


def test_attempt_one_threads_override_through(monkeypatch) -> None:
    from vzc.pipeline import _attempt as attempt_mod
    from vzc.pipeline._overrides import CollectionOverride

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
    from vzc.pipeline import _attempt as attempt_mod
    from vzc.pipeline._overrides import CollectionOverride

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
    assert result.success is True
    assert result.override_applied is True


# ---------------------------------------------------------------------------
# pending_granules behavior (state-based)
# ---------------------------------------------------------------------------


def test_pending_attempts_skips_pairs_already_in_results(
    tmp_state_path: Path, tmp_results_dir: Path
):
    """The state.pending_granules antijoin against the Parquet log keeps the resume guarantee."""
    from vzc.pipeline._attempt import _pending_attempts
    from vzc.pipeline._attempt import _SCHEMA

    state = make_state(
        collections=[make_collection("C1")],
        granules=[
            make_granule("C1", "G1", s3_url="s3://b/1", https_url="https://b/1"),
            make_granule(
                "C1",
                "G2",
                s3_url="s3://b/2",
                https_url="https://b/2",
                stratification_bin=1,
            ),
        ],
    )
    save_state(state, tmp_state_path)

    # Pretend G1 is already in the Parquet log
    cols = {f.name: [] for f in _SCHEMA}
    cols["collection_concept_id"].append("C1")
    cols["granule_concept_id"].append("G1")
    for f in _SCHEMA:
        if f.name in ("collection_concept_id", "granule_concept_id"):
            continue
        cols[f.name].append(None)
    shard_dir = tmp_results_dir / "DAAC=PODAAC"
    shard_dir.mkdir(parents=True)
    pq.write_table(pa.table(cols, schema=_SCHEMA), shard_dir / "part-0.parquet")

    state = load_state(tmp_state_path)
    pending = _pending_attempts(state, "direct", tmp_results_dir)
    assert [p["granule_concept_id"] for p in pending] == ["G2"]


def test_single_granule_attempt_short_circuits_uncached_external(
    tmp_path: Path, monkeypatch
) -> None:
    """External-mode attempts on uncached granules must NOT invoke the parser.

    Without this preflight, h5py's C-level get_eof callback drives our
    ReadOnlyCacheStore.head() → FileNotFoundError, which it then mangles
    across the C/Python boundary into an opaque
    ``SystemError: <lru_cache_wrapper> returned a result with an exception set``.
    """
    from vzc.pipeline import _attempt as attempt_mod
    from vzc.pipeline._attempt import GranuleInfo, SingleGranuleAttempt
    from vzc.pipeline._stores import StoreCache

    def _fail(*a, **kw):
        raise AssertionError("dispatch_parser must not be called for uncached granule")

    monkeypatch.setattr(attempt_mod, "dispatch_parser", _fail)

    cache = StoreCache(access="external", cache_dir=tmp_path)
    result = SingleGranuleAttempt(
        granule=GranuleInfo(
            url="https://x.example/missing.nc",
            family=FormatFamily.NETCDF4,
            collection_concept_id="C1",
            granule_concept_id="G1",
            daac="PODAAC",
            provider="POCLOUD",
        ),
        cache=cache,
        timeout_s=10,
    ).run()
    assert result.parse_success is False
    assert result.parse_error_type == "NotPrefetched"
    assert "prefetch" in (result.parse_error_message or "")
    assert result.collection_concept_id == "C1"
    assert result.granule_concept_id == "G1"
    assert result.daac == "PODAAC"
    assert result.format_family == "NetCDF4"


def test_run_attempt_external_requires_cache_dir(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    save_state(make_state(), state_path)

    with pytest.raises(ValueError, match="cache_dir"):
        _run_attempt(
            load_state(state_path),
            access="external",
            results_dir=tmp_path / "results",
            cache_dir=None,
            timeout_s=60,
            skip_override_validation=True,
        )
