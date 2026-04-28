from unittest.mock import MagicMock

import pytest
import requests

from vzc.cmr._popularity import (
    all_top_collection_ids,
    fetch_usage_metrics,
    top_collection_ids,
    top_collection_ids_total,
)


def _setup(monkeypatch, *, metrics, by_provider):
    """Stub both CMR endpoints. ``metrics`` and ``by_provider`` use compact tuples."""
    fetch_usage_metrics.cache_clear()

    def _resp(payload):
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json.return_value = payload
        return r

    monkeypatch.setattr(
        "vzc.cmr._popularity.requests.get",
        MagicMock(
            return_value=_resp(
                [
                    {"short-name": s, "version": v, "access-count": c}
                    for s, v, c in metrics
                ]
            )
        ),
    )

    def fake_post(url, data=None, timeout=None, **_):
        entries = by_provider.get(data["provider"], [])
        if entries == "raise":
            raise requests.HTTPError("500")
        return _resp(
            {
                "feed": {
                    "entry": [
                        {"id": cid, "short_name": s, "version_id": v}
                        for cid, s, v in entries
                    ]
                }
            }
        )

    monkeypatch.setattr("vzc.cmr._popularity.requests.post", fake_post)


def test_top_collection_ids_joins_metrics(monkeypatch):
    _setup(
        monkeypatch,
        metrics=[("FOO", "1", 1234), ("BAR", "2", 56)],
        by_provider={
            "GES_DISC": [
                ("C1", "FOO", "1"),
                ("C2", "BAR", "2"),
                ("C3", "ORPHAN", "1"),  # not in metrics → score=None
            ]
        },
    )
    assert top_collection_ids("GES_DISC", num=3) == [
        ("C1", 1234),
        ("C2", 56),
        ("C3", None),
    ]


def test_top_collection_ids_rejects_over_max(monkeypatch):
    _setup(monkeypatch, metrics=[], by_provider={})
    with pytest.raises(ValueError):
        top_collection_ids("GES_DISC", num=3000)


def test_fetch_usage_metrics_handles_missing_version_and_network_failure(monkeypatch):
    _setup(monkeypatch, metrics=[], by_provider={})
    # Live API occasionally omits `version`; defaults to "N/A".
    r = MagicMock()
    r.raise_for_status = MagicMock()
    r.json.return_value = [{"short-name": "X", "access-count": 9}]
    monkeypatch.setattr("vzc.cmr._popularity.requests.get", MagicMock(return_value=r))
    fetch_usage_metrics.cache_clear()
    assert fetch_usage_metrics() == {("X", "N/A"): 9}

    # Network failure → empty map (degraded, not failed).
    fetch_usage_metrics.cache_clear()
    monkeypatch.setattr(
        "vzc.cmr._popularity.requests.get",
        MagicMock(side_effect=requests.ConnectionError("offline")),
    )
    assert fetch_usage_metrics() == {}


def test_all_top_collection_ids_sorts_globally_and_skips_5xx(monkeypatch):
    _setup(
        monkeypatch,
        metrics=[("A", "1", 100), ("B", "1", 500), ("C", "1", 250)],
        by_provider={
            "GES_DISC": [("C-A", "A", "1"), ("C-B", "B", "1")],
            "POCLOUD": [("C-C", "C", "1")],
            "BAD": "raise",
        },
    )
    assert all_top_collection_ids(
        ["GES_DISC", "POCLOUD", "BAD"], num_per_provider=2
    ) == [("C-B", 500), ("C-C", 250), ("C-A", 100)]


def test_top_collection_ids_total_is_true_global_top_n(monkeypatch):
    """`--top N` lets one provider dominate; selection is global top-N by score."""
    _setup(
        monkeypatch,
        metrics=[
            ("P1", "1", 1000),
            ("P2", "1", 900),
            ("P3", "1", 800),
            ("G1", "1", 50),
            ("G2", "1", 40),
        ],
        by_provider={
            "POCLOUD": [("C-P1", "P1", "1"), ("C-P2", "P2", "1"), ("C-P3", "P3", "1")],
            "GES_DISC": [("C-G1", "G1", "1"), ("C-G2", "G2", "1")],
        },
    )
    assert top_collection_ids_total(["POCLOUD", "GES_DISC"], num_total=3) == [
        ("C-P1", 1000),
        ("C-P2", 900),
        ("C-P3", 800),
    ]


def test_top_collection_ids_total_orders_unscored_last(monkeypatch):
    _setup(
        monkeypatch,
        metrics=[("SCORED", "1", 1)],
        by_provider={
            "GES_DISC": [("C-NONE", "MISSING", "1"), ("C-SCORED", "SCORED", "1")]
        },
    )
    pairs = top_collection_ids_total(["GES_DISC"], num_total=5)
    assert pairs == [("C-SCORED", 1), ("C-NONE", None)]


def test_top_collection_ids_total_empty_inputs(monkeypatch):
    _setup(monkeypatch, metrics=[], by_provider={})
    assert top_collection_ids_total(["GES_DISC"], num_total=0) == []
    assert top_collection_ids_total([], num_total=10) == []
