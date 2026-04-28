from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from vzc.cmr import _opendap as opendap


@pytest.fixture(autouse=True)
def _clear_service_cache():
    """Reset the lru_cache between tests so each can stub its own response."""
    opendap.cloud_opendap_service_ids.cache_clear()
    yield
    opendap.cloud_opendap_service_ids.cache_clear()


def _services_payload(items: list[tuple[str, str]]) -> dict[str, Any]:
    """Build a fake CMR services.umm_json response from (concept_id, url) pairs."""
    return {
        "items": [
            {"meta": {"concept-id": cid}, "umm": {"URL": {"URLValue": url}}}
            for cid, url in items
        ]
    }


def test_cloud_opendap_service_ids_filters_to_hyrax_url(monkeypatch):
    """Only records whose URL matches the cloud Hyrax URL are kept."""
    payload = _services_payload(
        [
            ("S-OFFICIAL", opendap.CLOUD_OPENDAP_URL),
            ("S-UNOFFICIAL", opendap.CLOUD_OPENDAP_URL + "/"),  # trailing slash variant
            ("S-OTHER", "https://opendap.example.gov"),
            ("S-EMPTY", ""),
        ]
    )
    fake = MagicMock()
    fake.json.return_value = payload
    fake.raise_for_status.return_value = None
    monkeypatch.setattr(opendap.requests, "get", lambda *a, **kw: fake)

    ids = opendap.cloud_opendap_service_ids()
    assert ids == frozenset({"S-OFFICIAL", "S-UNOFFICIAL"})


def test_cloud_opendap_service_ids_is_cached(monkeypatch):
    """The CMR call is made at most once per process (lru_cache)."""
    fake = MagicMock()
    fake.json.return_value = _services_payload([("S1", opendap.CLOUD_OPENDAP_URL)])
    fake.raise_for_status.return_value = None
    call_count = {"n": 0}

    def fake_get(*a, **kw):
        call_count["n"] += 1
        return fake

    monkeypatch.setattr(opendap.requests, "get", fake_get)
    opendap.cloud_opendap_service_ids()
    opendap.cloud_opendap_service_ids()
    assert call_count["n"] == 1


def test_collection_has_cloud_opendap_matches_any_associated_id():
    coll = {"meta": {"associations": {"services": ["S-OFFICIAL", "S-OTHER"]}}}
    assert opendap.collection_has_cloud_opendap(coll, frozenset({"S-OFFICIAL"})) is True
    assert opendap.collection_has_cloud_opendap(coll, frozenset({"S-NOMATCH"})) is False


def test_collection_has_cloud_opendap_handles_missing_associations():
    """A collection with no service associations is unambiguously False."""
    assert opendap.collection_has_cloud_opendap({}, frozenset({"S1"})) is False
    assert (
        opendap.collection_has_cloud_opendap(
            {"meta": {"associations": {}}}, frozenset({"S1"})
        )
        is False
    )
    assert (
        opendap.collection_has_cloud_opendap(
            {"meta": {"associations": {"services": None}}}, frozenset({"S1"})
        )
        is False
    )


@pytest.mark.parametrize(
    "data_url, expected",
    [
        ("s3://bucket/path/file.h5", "s3://bucket/path/file.h5.dmrpp"),
        ("https://x/y/file.nc", "https://x/y/file.nc.dmrpp"),
        (None, None),
        ("", None),
    ],
)
def test_dmrpp_url_for(data_url, expected):
    assert opendap.dmrpp_url_for(data_url) == expected


def test_verify_dmrpp_exists_truthy_on_2xx(monkeypatch):
    sess = MagicMock()
    sess.head.return_value = MagicMock(status_code=200)
    assert opendap.verify_dmrpp_exists("https://x/y.dmrpp", session=sess) is True


@pytest.mark.parametrize("status", [403, 404, 500])
def test_verify_dmrpp_exists_falsy_on_non_2xx(monkeypatch, status):
    sess = MagicMock()
    sess.head.return_value = MagicMock(status_code=status)
    assert opendap.verify_dmrpp_exists("https://x/y.dmrpp", session=sess) is False


def test_verify_dmrpp_exists_falsy_on_network_error(monkeypatch):
    sess = MagicMock()
    sess.head.side_effect = requests.ConnectionError("boom")
    assert opendap.verify_dmrpp_exists("https://x/y.dmrpp", session=sess) is False
