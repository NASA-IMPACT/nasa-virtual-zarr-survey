from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from nasa_virtual_zarr_survey.auth import DAACStoreCache


def test_cache_fetches_once(monkeypatch):
    calls = {"n": 0}

    def fake_get_creds(provider):
        calls["n"] += 1
        return {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"}

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials", fake_get_creds
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth._build_store", lambda creds, provider: object()
    )

    cache = DAACStoreCache()
    store1 = cache.get_store("PODAAC")
    store2 = cache.get_store("PODAAC")
    assert store1 is store2
    assert calls["n"] == 1


def test_cache_refreshes_after_ttl(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: (calls.__setitem__("n", calls["n"] + 1), {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"})[1],
    )
    monkeypatch.setattr("nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None)
    monkeypatch.setattr("nasa_virtual_zarr_survey.auth._build_store", lambda creds, provider: object())

    cache = DAACStoreCache(ttl=timedelta(minutes=50))
    cache.get_store("PODAAC")
    # Force expiry
    cache._entries["PODAAC"] = cache._entries["PODAAC"]._replace(
        minted_at=datetime.now(timezone.utc) - timedelta(hours=2)
    )
    cache.get_store("PODAAC")
    assert calls["n"] == 2


def test_login_called_once_across_multiple_daacs(monkeypatch):
    login_calls = {"n": 0}
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login",
        lambda **_: login_calls.__setitem__("n", login_calls["n"] + 1),
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"},
    )
    monkeypatch.setattr("nasa_virtual_zarr_survey.auth._build_store", lambda creds, provider: object())

    cache = DAACStoreCache()
    cache.get_store("PODAAC")
    cache.get_store("NSIDC_ECS")
    assert login_calls["n"] == 1


def test_cache_raises_on_empty_creds(monkeypatch):
    monkeypatch.setattr("nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None)
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: {},
    )

    from nasa_virtual_zarr_survey.auth import AuthUnavailable

    cache = DAACStoreCache()
    with pytest.raises(AuthUnavailable):
        cache.get_store("UNKNOWN_PROVIDER")


def test_store_cache_external_mode(monkeypatch):
    from nasa_virtual_zarr_survey.auth import StoreCache

    class FakeAuth:
        token = {"access_token": "BEARER_XYZ"}

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.__auth__", FakeAuth, raising=False
    )

    created: list[dict] = []

    class FakeAiohttpStore:
        def __init__(self, base_url, headers=None):
            created.append({"base_url": base_url, "headers": headers})
            self.base_url = base_url
            self.headers = headers

    monkeypatch.setattr(
        "obspec_utils.stores.AiohttpStore", FakeAiohttpStore
    )

    cache = StoreCache(access="external")
    s1 = cache.get_store(provider="PODAAC", url="https://host-a.example/path/one.nc")
    s2 = cache.get_store(provider="PODAAC", url="https://host-a.example/path/two.nc")
    s3 = cache.get_store(provider="PODAAC", url="https://host-b.example/path/x.nc")

    assert s1 is s2            # same host => same store
    assert s3 is not s1         # different host => different store
    assert len(created) == 2
    assert created[0]["base_url"] == "https://host-a.example"
    assert created[0]["headers"] == {"Authorization": "Bearer BEARER_XYZ"}
    assert created[1]["base_url"] == "https://host-b.example"


def test_store_cache_direct_delegates_to_daac_cache(monkeypatch):
    from nasa_virtual_zarr_survey.auth import StoreCache

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"},
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth._build_s3_store", lambda creds, provider: f"S3({provider})"
    )

    cache = StoreCache(access="direct")
    s = cache.get_store(provider="PODAAC", url="s3://podaac-bucket/x.nc")
    assert s == "S3(PODAAC)"
