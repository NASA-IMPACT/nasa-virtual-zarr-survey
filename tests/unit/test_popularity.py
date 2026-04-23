from unittest.mock import MagicMock

import requests

from nasa_virtual_zarr_survey.popularity import (
    all_top_collection_ids,
    top_collection_ids,
)


def test_top_collection_ids_parses_response(monkeypatch):
    fake_response = MagicMock()
    fake_response.json.return_value = {
        "feed": {
            "entry": [
                {"id": "C1-PODAAC"},
                {"id": "C2-PODAAC"},
                {"id": "C3-PODAAC"},
            ]
        }
    }
    fake_response.raise_for_status = MagicMock()
    fake_post = MagicMock(return_value=fake_response)
    monkeypatch.setattr("nasa_virtual_zarr_survey.popularity.requests.post", fake_post)

    ids = top_collection_ids("PODAAC", num=3)
    assert ids == ["C1-PODAAC", "C2-PODAAC", "C3-PODAAC"]

    args, kwargs = fake_post.call_args
    assert kwargs["data"]["provider"] == "PODAAC"
    assert kwargs["data"]["page_size"] == 3
    assert kwargs["data"]["sort_key[]"] == "-usage_score"


def test_top_collection_ids_rejects_over_max():
    import pytest

    with pytest.raises(ValueError):
        top_collection_ids("PODAAC", num=3000)


def test_all_top_collection_ids_concatenates(monkeypatch):
    call_count = {"n": 0}
    providers_ids = {"PODAAC": ["C1", "C2"], "NSIDC_CPRD": ["C3"]}

    def fake_top(provider, num=100):
        call_count["n"] += 1
        return providers_ids[provider]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.popularity.top_collection_ids", fake_top
    )
    ids = all_top_collection_ids(["PODAAC", "NSIDC_CPRD"], num_per_provider=2)
    assert ids == ["C1", "C2", "C3"]
    assert call_count["n"] == 2


def test_all_top_collection_ids_skips_failed_providers(monkeypatch):
    def fake_top(provider, num=100):
        if provider == "BAD":
            err = requests.HTTPError("500 error")
            err.response = MagicMock(status_code=500)
            raise err
        return ["C1", "C2"]

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.popularity.top_collection_ids", fake_top
    )
    ids = all_top_collection_ids(["GOOD", "BAD", "OTHER"], num_per_provider=2)
    assert ids == ["C1", "C2", "C1", "C2"]  # BAD skipped
