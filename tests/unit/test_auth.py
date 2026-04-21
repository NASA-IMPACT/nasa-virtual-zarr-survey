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
