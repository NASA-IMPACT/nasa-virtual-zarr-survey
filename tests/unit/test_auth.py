from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from vzc.pipeline._stores import DAACStoreCache


def test_cache_fetches_creds_once_per_provider(monkeypatch):
    calls = {"n": 0}

    def fake_get_creds(provider):
        calls["n"] += 1
        return {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"}

    monkeypatch.setattr(
        "vzc.pipeline._stores.earthaccess.get_s3_credentials", fake_get_creds
    )
    monkeypatch.setattr("vzc.pipeline._stores.earthaccess.login", lambda **_: None)

    built: list = []

    def fake_build_s3_store(creds, bucket):
        built.append((creds, bucket))
        return f"S3({bucket})"

    monkeypatch.setattr("vzc.pipeline._stores._build_s3_store", fake_build_s3_store)

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
        "vzc.pipeline._stores.earthaccess.get_s3_credentials", fake_get_creds
    )
    monkeypatch.setattr("vzc.pipeline._stores.earthaccess.login", lambda **_: None)
    monkeypatch.setattr(
        "vzc.pipeline._stores._build_s3_store",
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
        "vzc.pipeline._stores.earthaccess.login",
        lambda **_: login_calls.__setitem__("n", login_calls["n"] + 1),
    )
    monkeypatch.setattr(
        "vzc.pipeline._stores.earthaccess.get_s3_credentials",
        lambda provider: {
            "accessKeyId": "AK",
            "secretAccessKey": "SK",
            "sessionToken": "TK",
        },
    )
    monkeypatch.setattr(
        "vzc.pipeline._stores._build_s3_store",
        lambda creds, bucket: f"S3({bucket})",
    )

    cache = DAACStoreCache()
    cache.get_store(provider="PODAAC", bucket="podaac-ops")
    cache.get_store(provider="NSIDC_CPRD", bucket="nsidc-cprd")
    assert login_calls["n"] == 1


def test_cache_raises_on_empty_creds(monkeypatch):
    monkeypatch.setattr("vzc.pipeline._stores.earthaccess.login", lambda **_: None)
    monkeypatch.setattr(
        "vzc.pipeline._stores.earthaccess.get_s3_credentials",
        lambda provider: {},
    )

    from vzc.pipeline._stores import AuthUnavailable

    cache = DAACStoreCache()
    with pytest.raises(AuthUnavailable):
        cache.get_store(provider="UNKNOWN_PROVIDER", bucket="no-such-bucket")


def test_store_cache_direct_extracts_bucket_from_url(monkeypatch):
    from vzc.pipeline._stores import StoreCache

    calls = []

    def fake_get_creds(provider):
        return {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"}

    def fake_build_s3_store(creds, bucket):
        calls.append(("build", bucket))
        return f"S3({bucket})"

    monkeypatch.setattr("vzc.pipeline._stores.earthaccess.login", lambda **_: None)
    monkeypatch.setattr(
        "vzc.pipeline._stores.earthaccess.get_s3_credentials", fake_get_creds
    )
    monkeypatch.setattr("vzc.pipeline._stores._build_s3_store", fake_build_s3_store)

    cache = StoreCache(access="direct")
    s1 = cache.get_store(provider="PODAAC", url="s3://prod-lads/VNP02DNB/file.nc")
    s2 = cache.get_store(provider="PODAAC", url="s3://other-bucket/other.nc")
    assert s1 == "S3(prod-lads)"
    assert s2 == "S3(other-bucket)"
    assert calls == [("build", "prod-lads"), ("build", "other-bucket")]


def test_store_cache_external_without_cache_dir_raises(monkeypatch):
    """access='external' is cache-only and requires cache_dir."""
    from vzc.pipeline._stores import AuthUnavailable, StoreCache

    cache = StoreCache(access="external")
    with pytest.raises(AuthUnavailable, match="cache-only"):
        cache.get_store(provider="PODAAC", url="https://h.example/path.nc")


def test_store_cache_direct_returns_s3_store(monkeypatch):
    """access='direct' returns a live S3Store (no cache wrapping)."""
    from vzc.pipeline._stores import StoreCache

    monkeypatch.setattr("vzc.pipeline._stores.earthaccess.login", lambda **_: None)
    monkeypatch.setattr(
        "vzc.pipeline._stores.earthaccess.get_s3_credentials",
        lambda provider: {
            "accessKeyId": "AK",
            "secretAccessKey": "SK",
            "sessionToken": "TK",
        },
    )
    monkeypatch.setattr(
        "vzc.pipeline._stores._build_s3_store",
        lambda creds, bucket: f"S3({bucket})",
    )

    cache = StoreCache(access="direct")
    s = cache.get_store(provider="PODAAC", url="s3://b/key.nc")
    assert s == "S3(b)"


def test_store_cache_external_returns_read_only_cache_store(monkeypatch, tmp_path):
    """access='external' + cache_dir returns a ReadOnlyCacheStore (no live HTTPS)."""
    from vzc.pipeline._stores import StoreCache
    from vzc.pipeline._stores import ReadOnlyCacheStore

    cache = StoreCache(access="external", cache_dir=tmp_path)
    s = cache.get_store(provider="PODAAC", url="https://h.example/path.nc")
    assert isinstance(s, ReadOnlyCacheStore)
    # Same host → same instance (cached per prefix).
    s2 = cache.get_store(provider="PODAAC", url="https://h.example/other.nc")
    assert s is s2


def test_make_https_store_returns_live_http_store(monkeypatch):
    """make_https_store builds a live HTTPStore (used by prefetch + investigate)."""
    from vzc.pipeline._stores import make_https_store

    class FakeAuth:
        token = {"access_token": "BEARER_XYZ"}

    monkeypatch.setattr("vzc.pipeline._stores.earthaccess.login", lambda **_: None)
    monkeypatch.setattr(
        "vzc.pipeline._stores.earthaccess.__auth__", FakeAuth, raising=False
    )

    captured: dict = {}

    class FakeHTTPStore:
        @classmethod
        def from_url(cls, url, *, client_options=None, **_):  # type: ignore[no-untyped-def]
            captured["url"] = url
            captured["client_options"] = client_options
            return cls()

    monkeypatch.setattr("obstore.store.HTTPStore", FakeHTTPStore)

    make_https_store("https://host.example/path/file.nc")
    assert captured["url"] == "https://host.example"
    assert captured["client_options"] == {
        "default_headers": {"Authorization": "Bearer BEARER_XYZ"}
    }
