"""Tests for the run_history rendering pipeline.

The standalone `history` subcommand has been removed; rendering is now
exercised end-to-end through `report --history`. These tests call
``run_history()`` directly, which keeps them fast and isolates the
rendering surface from the broader report flow.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from vzc.render._history import run_history


def _summary(date_: str, kind: str = "release", label: str | None = None) -> dict:
    return {
        "schema_version": 8,
        "generated_at": "2026-04-26T00:00:00+00:00",
        "survey_tool_version": "0.1.0",
        "virtualizarr_version": "1.3.0",
        "zarr_version": "3.0.8",
        "xarray_version": "2026.1.0",
        "sampling_mode": "top=1",
        "snapshot_date": date_,
        "snapshot_kind": kind,
        "label": label,
        "description": None,
        "locked_sample_sha256": "abc",
        "verdicts": [],
        "parse_taxonomy": {},
        "dataset_taxonomy": {},
        "datatree_taxonomy": {},
        "cubability_results": {},
        "other_parse_errors": [],
        "other_dataset_errors": [],
        "other_datatree_errors": [],
        "skipped_by_format": [],
    }


def _empty_intros(tmp_path: Path) -> Path:
    """An empty feature-introductions TOML; some tests don't care about intros."""
    p = tmp_path / "intros.toml"
    p.write_text("")
    return p


def test_history_renders_minimal_page(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    (history_dir / "2026-02-15.summary.json").write_text(
        json.dumps(_summary("2026-02-15"))
    )
    (history_dir / "2026-04-01.summary.json").write_text(
        json.dumps(_summary("2026-04-01"))
    )

    out = tmp_path / "history.md"
    run_history(history_dir, out, intros_path=_empty_intros(tmp_path))
    text = out.read_text()
    assert "# Coverage over time" in text
    assert "2026-02-15" in text
    assert "2026-04-01" in text


def test_load_all_sorts_null_dates_last(tmp_path: Path) -> None:
    """Summaries with snapshot_date=None must sort *after* dated ones,
    so a hand-edited or in-progress digest doesn't render as the
    leftmost (earliest) point on trend charts."""
    from vzc.render._history import _load_all

    history_dir = tmp_path / "history"
    history_dir.mkdir()
    s_early = _summary("2026-01-01")
    s_late = _summary("2026-03-01")
    s_undated = _summary("2026-02-01")
    s_undated["snapshot_date"] = None
    (history_dir / "early.summary.json").write_text(json.dumps(s_early))
    (history_dir / "undated.summary.json").write_text(json.dumps(s_undated))
    (history_dir / "late.summary.json").write_text(json.dumps(s_late))

    summaries = _load_all(history_dir)
    dates = [s.snapshot_date for s in summaries]
    assert dates == ["2026-01-01", "2026-03-01", None]


def test_history_rejects_v5_summary(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    (history_dir / "old.summary.json").write_text(json.dumps({"schema_version": 5}))

    with pytest.raises(Exception):
        run_history(
            history_dir, tmp_path / "history.md", intros_path=_empty_intros(tmp_path)
        )


def test_history_warns_on_locked_sample_drift(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    s1 = _summary("2026-02-15")
    s2 = _summary("2026-04-01")
    s2["locked_sample_sha256"] = "DIFFERENT"
    (history_dir / "2026-02-15.summary.json").write_text(json.dumps(s1))
    (history_dir / "2026-04-01.summary.json").write_text(json.dumps(s2))

    warning = run_history(
        history_dir, tmp_path / "history.md", intros_path=_empty_intros(tmp_path)
    )
    assert warning is not None
    assert "locked_sample_sha256" in warning or "drift" in warning.lower()


def test_history_funnel_html_emitted(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    s1 = _summary("2026-02-15")
    s1["verdicts"] = [
        {
            "concept_id": "C1-T",
            "daac": "X",
            "format_family": "NetCDF4",
            "skip_reason": None,
            "parse_verdict": "all_pass",
            "dataset_verdict": "all_fail",
            "datatree_verdict": "all_fail",
            "top_bucket": "",
        }
    ]
    s2 = _summary("2026-04-01")
    s2["verdicts"] = [
        {
            "concept_id": "C1-T",
            "daac": "X",
            "format_family": "NetCDF4",
            "skip_reason": None,
            "parse_verdict": "all_pass",
            "dataset_verdict": "all_pass",
            "datatree_verdict": "all_pass",
            "top_bucket": "",
        }
    ]
    (history_dir / "2026-02-15.summary.json").write_text(json.dumps(s1))
    (history_dir / "2026-04-01.summary.json").write_text(json.dumps(s2))

    out = tmp_path / "history.md"
    run_history(history_dir, out, intros_path=_empty_intros(tmp_path))
    funnel_html = out.parent / "history" / "figures" / "funnel_over_time.html"
    assert funnel_html.exists(), (
        f"expected {funnel_html}; got: {list(out.parent.rglob('*'))}"
    )
    assert "Funnel over time" in out.read_text()


def test_history_bucket_trend_emitted(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    s1 = _summary("2026-02-15")
    s1["parse_taxonomy"] = {"UNDEFINED_FILL_VALUE": [10, 3]}
    s2 = _summary("2026-04-01")
    s2["parse_taxonomy"] = {"UNDEFINED_FILL_VALUE": [3, 1]}
    (history_dir / "2026-02-15.summary.json").write_text(json.dumps(s1))
    (history_dir / "2026-04-01.summary.json").write_text(json.dumps(s2))

    out = tmp_path / "history.md"
    run_history(history_dir, out, intros_path=_empty_intros(tmp_path))
    bucket_html = out.parent / "history" / "figures" / "bucket_trend.html"
    assert bucket_html.exists()


def test_history_state_transitions(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()

    def _verdict(dataset_v: str) -> dict:
        return {
            "concept_id": "C1-T",
            "daac": "X",
            "format_family": "NetCDF4",
            "skip_reason": None,
            "parse_verdict": "all_pass",
            "dataset_verdict": dataset_v,
            "datatree_verdict": "not_attempted",
            "top_bucket": "",
        }

    s1 = _summary("2026-02-15")
    s1["verdicts"] = [_verdict("all_fail")]
    s2 = _summary("2026-04-01")
    s2["verdicts"] = [_verdict("all_pass")]
    (history_dir / "2026-02-15.summary.json").write_text(json.dumps(s1))
    (history_dir / "2026-04-01.summary.json").write_text(json.dumps(s2))

    out = tmp_path / "history.md"
    run_history(history_dir, out, intros_path=_empty_intros(tmp_path))
    text = out.read_text()
    assert "C1-T" in text
    assert "Newly passing" in text


def test_history_preview_section(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    s_release = _summary("2026-02-15")
    s_preview = _summary("2026-04-26", kind="preview", label="variable-chunking")
    s_preview["description"] = "Coordinated branches"
    (history_dir / "2026-02-15.summary.json").write_text(json.dumps(s_release))
    (history_dir / "2026-04-26-variable-chunking.summary.json").write_text(
        json.dumps(s_preview)
    )

    out = tmp_path / "history.md"
    run_history(history_dir, out, intros_path=_empty_intros(tmp_path))
    text = out.read_text()
    assert "## Preview snapshots" in text
    assert "variable-chunking" in text
    assert "Coordinated branches" in text


def test_history_feature_introductions_list(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    (history_dir / "2026-02-15.summary.json").write_text(
        json.dumps(_summary("2026-02-15"))
    )

    intros_path = tmp_path / "intros.toml"
    intros_path.write_text(
        "[has_datatree]\n"
        'phases = ["datatree"]\n'
        'first_in_vz = "2.0.0"\n'
        'introduced = "2026-03-15"\n'
        'description = "ManifestStore.to_virtual_datatree()"\n'
    )

    out = tmp_path / "history.md"
    run_history(history_dir, out, intros_path=intros_path)
    text = out.read_text()
    assert "## Feature introductions" in text
    assert "has_datatree" in text
    assert "2.0.0" in text
    assert "ManifestStore.to_virtual_datatree" in text


def test_history_methodology_footnote(tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    (history_dir / "2026-02-15.summary.json").write_text(
        json.dumps(_summary("2026-02-15"))
    )
    out = tmp_path / "history.md"
    run_history(history_dir, out, intros_path=_empty_intros(tmp_path))
    text = out.read_text()
    assert "## Methodology" in text
    assert "abc" in text  # the locked_sample_sha256 prefix from _summary()
