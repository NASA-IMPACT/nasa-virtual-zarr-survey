from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from nasa_virtual_zarr_survey.auth import DAACStoreCache


def test_cache_fetches_creds_once_per_provider(monkeypatch):
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

    built: list = []

    def fake_build_s3_store(creds, bucket):
        built.append((creds, bucket))
        return f"S3({bucket})"

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth._build_s3_store", fake_build_s3_store
    )

    cache = DAACStoreCache()
    s1 = cache.get_store(provider="PODAAC", bucket="podaac-ops")
    s2 = cache.get_store(provider="PODAAC", bucket="podaac-ops")  # same bucket, cached
    s3 = cache.get_store(
        provider="PODAAC", bucket="other-bucket"
    )  # new bucket, new store
    assert s1 == "S3(podaac-ops)"
    assert s1 is s2
    assert s3 == "S3(other-bucket)"
    assert calls["n"] == 1  # creds fetched once
    assert len(built) == 2  # two distinct buckets -> two stores


def test_cache_refreshes_creds_after_ttl(monkeypatch):
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
        "nasa_virtual_zarr_survey.auth._build_s3_store",
        lambda creds, bucket: f"S3({bucket})",
    )

    cache = DAACStoreCache(ttl=timedelta(minutes=50))
    cache.get_store(provider="PODAAC", bucket="podaac-ops")
    # Force expiry on the creds entry
    entry = cache._creds["PODAAC"]
    cache._creds["PODAAC"] = entry._replace(
        minted_at=datetime.now(timezone.utc) - timedelta(hours=2)
    )
    cache.get_store(provider="PODAAC", bucket="podaac-ops")
    assert calls["n"] == 2


def test_login_called_once_across_multiple_providers(monkeypatch):
    login_calls = {"n": 0}
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login",
        lambda **_: login_calls.__setitem__("n", login_calls["n"] + 1),
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: {
            "accessKeyId": "AK",
            "secretAccessKey": "SK",
            "sessionToken": "TK",
        },
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth._build_s3_store",
        lambda creds, bucket: f"S3({bucket})",
    )

    cache = DAACStoreCache()
    cache.get_store(provider="PODAAC", bucket="podaac-ops")
    cache.get_store(provider="NSIDC_CPRD", bucket="nsidc-cprd")
    assert login_calls["n"] == 1


def test_cache_raises_on_empty_creds(monkeypatch):
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: {},
    )

    from nasa_virtual_zarr_survey.auth import AuthUnavailable

    cache = DAACStoreCache()
    with pytest.raises(AuthUnavailable):
        cache.get_store(provider="UNKNOWN_PROVIDER", bucket="no-such-bucket")


def test_store_cache_direct_extracts_bucket_from_url(monkeypatch):
    from nasa_virtual_zarr_survey.auth import StoreCache

    calls = []

    def fake_get_creds(provider):
        return {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"}

    def fake_build_s3_store(creds, bucket):
        calls.append(("build", bucket))
        return f"S3({bucket})"

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials", fake_get_creds
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth._build_s3_store", fake_build_s3_store
    )

    cache = StoreCache(access="direct")
    s1 = cache.get_store(provider="PODAAC", url="s3://prod-lads/VNP02DNB/file.nc")
    s2 = cache.get_store(provider="PODAAC", url="s3://other-bucket/other.nc")
    assert s1 == "S3(prod-lads)"
    assert s2 == "S3(other-bucket)"
    assert calls == [("build", "prod-lads"), ("build", "other-bucket")]


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

    class FakeHTTPStore:
        def __init__(self, url: str, client_options: dict | None = None) -> None:
            created.append({"url": url, "client_options": client_options})
            self.url = url
            self.client_options = client_options

        @classmethod
        def from_url(
            cls, url: str, *, client_options: dict | None = None, **_
        ) -> "FakeHTTPStore":
            return cls(url, client_options=client_options)

    monkeypatch.setattr("obstore.store.HTTPStore", FakeHTTPStore)

    cache = StoreCache(access="external")
    s1 = cache.get_store(provider="PODAAC", url="https://host-a.example/path/one.nc")
    s2 = cache.get_store(provider="PODAAC", url="https://host-a.example/path/two.nc")
    s3 = cache.get_store(provider="PODAAC", url="https://host-b.example/path/x.nc")

    assert s1 is s2
    assert s3 is not s1
    assert len(created) == 2
    assert created[0]["url"] == "https://host-a.example"
    assert created[0]["client_options"] == {
        "default_headers": {"Authorization": "Bearer BEARER_XYZ"}
    }
    assert created[1]["url"] == "https://host-b.example"


def test_store_cache_no_cache_returns_underlying_store(monkeypatch, tmp_path):
    from nasa_virtual_zarr_survey.auth import StoreCache
    from nasa_virtual_zarr_survey.cache import DiskCachingReadableStore

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: {
            "accessKeyId": "AK",
            "secretAccessKey": "SK",
            "sessionToken": "TK",
        },
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth._build_s3_store",
        lambda creds, bucket: f"S3({bucket})",
    )

    cache = StoreCache(access="direct")  # cache_dir=None
    s = cache.get_store(provider="PODAAC", url="s3://b/key.nc")
    assert s == "S3(b)"
    assert not isinstance(s, DiskCachingReadableStore)


def test_store_cache_with_cache_dir_wraps_s3_store(monkeypatch, tmp_path):
    from pathlib import Path

    from nasa_virtual_zarr_survey.auth import StoreCache
    from nasa_virtual_zarr_survey.cache import DiskCachingReadableStore

    assert isinstance(tmp_path, Path)

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.get_s3_credentials",
        lambda provider: {
            "accessKeyId": "AK",
            "secretAccessKey": "SK",
            "sessionToken": "TK",
        },
    )
    # _build_s3_store must return something the wrapper treats as a real store.
    # We don't exercise reads here, so any object is fine — the wrapper only
    # touches it on get/get_range/head, which we don't call.
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth._build_s3_store",
        lambda creds, bucket: object(),
    )

    cache = StoreCache(access="direct", cache_dir=tmp_path, cache_max_bytes=1_000_000)
    s = cache.get_store(provider="PODAAC", url="s3://my-bucket/key.nc")
    assert isinstance(s, DiskCachingReadableStore)
    # Same bucket -> same wrapped instance (cached by bucket).
    s2 = cache.get_store(provider="PODAAC", url="s3://my-bucket/other.nc")
    assert s is s2


def test_store_cache_with_cache_dir_wraps_https_store(monkeypatch, tmp_path):
    from pathlib import Path

    from nasa_virtual_zarr_survey.auth import StoreCache
    from nasa_virtual_zarr_survey.cache import DiskCachingReadableStore

    assert isinstance(tmp_path, Path)

    class FakeAuth:
        token = {"access_token": "BEARER_XYZ"}

    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.login", lambda **_: None
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.auth.earthaccess.__auth__", FakeAuth, raising=False
    )

    class FakeHTTPStore:
        @classmethod
        def from_url(cls, url, *, client_options=None, **_):  # type: ignore[no-untyped-def]
            inst = cls()
            inst.url = url
            return inst

    monkeypatch.setattr("obstore.store.HTTPStore", FakeHTTPStore)

    cache = StoreCache(access="external", cache_dir=tmp_path, cache_max_bytes=1_000_000)
    s = cache.get_store(provider="PODAAC", url="https://h.example/path.nc")
    assert isinstance(s, DiskCachingReadableStore)
    # Same host -> same wrapped instance.
    s2 = cache.get_store(provider="PODAAC", url="https://h.example/other.nc")
    assert s is s2
