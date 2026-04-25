"""EDL login + per-provider credential cache + per-bucket S3 store + per-hostname HTTPS store."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple
from urllib.parse import urlparse

import earthaccess

from nasa_virtual_zarr_survey.cache import CacheSizeTracker, DiskCachingReadableStore

if TYPE_CHECKING:
    from obstore.store import HTTPStore, S3Store


class AuthUnavailable(Exception):
    """Raised when credentials cannot be minted for a given provider."""


class _Creds(NamedTuple):
    creds: dict[str, str]
    minted_at: datetime


@dataclass
class DAACStoreCache:
    """Caches EDL-minted S3 credentials per CMR provider and builds per-bucket obstore S3Stores lazily.

    Credentials are cached per provider with a TTL (defaults to 50 minutes, below
    earthaccess's 1-hour expiry). A distinct S3Store is constructed for each
    distinct bucket the caller asks for, sharing the underlying credentials.
    """

    ttl: timedelta = timedelta(minutes=50)
    _logged_in: bool = False
    _creds: dict[str, _Creds] = field(default_factory=dict)
    _stores: dict[tuple[str, str], S3Store] = field(default_factory=dict)

    def _login(self) -> None:
        if not self._logged_in:
            earthaccess.login(strategy="netrc")
            self._logged_in = True

    def _get_creds(self, provider: str) -> dict[str, str]:
        now = datetime.now(timezone.utc)
        entry = self._creds.get(provider)
        if entry and now - entry.minted_at < self.ttl:
            return entry.creds
        self._login()
        fresh = earthaccess.get_s3_credentials(provider=provider)
        if not fresh or "accessKeyId" not in fresh:
            raise AuthUnavailable(
                f"earthaccess returned no S3 credentials for provider {provider!r}"
            )
        self._creds[provider] = _Creds(creds=fresh, minted_at=now)
        # Invalidate any cached stores for this provider since they hold stale creds.
        self._stores = {k: v for k, v in self._stores.items() if k[0] != provider}
        return fresh

    def get_store(self, *, provider: str, bucket: str) -> S3Store:
        """Return an obstore S3Store for (provider, bucket), building it on demand.

        Credentials are checked for freshness first; if they have expired, any
        previously cached stores for this provider are discarded before a new
        store is built with fresh credentials.
        """
        # Always go through _get_creds so that expired credentials cause cached
        # stores to be purged (via the invalidation in _get_creds) before we
        # attempt to look up a cached store.
        creds = self._get_creds(provider)
        key = (provider, bucket)
        store = self._stores.get(key)
        if store is not None:
            return store
        store = _build_s3_store(creds, bucket)
        self._stores[key] = store
        return store


def _build_s3_store(creds: dict[str, str], bucket: str) -> S3Store:
    """Construct an obstore S3Store for the given bucket using the given credentials."""
    from obstore.store import S3Store

    return S3Store(
        bucket=bucket,
        access_key_id=creds["accessKeyId"],
        secret_access_key=creds["secretAccessKey"],
        session_token=creds["sessionToken"],
        region="us-west-2",
    )


@dataclass
class StoreCache:
    """Unified dispatcher for 'direct' (S3) and 'external' (HTTPS) access modes.

    Optionally wraps every returned store in ``DiskCachingReadableStore`` so
    fetched bytes persist across runs.
    """

    access: Literal["direct", "external"] = "direct"
    cache_dir: Path | None = None
    cache_max_bytes: int = 50 * 1024**3
    _s3: DAACStoreCache = field(default_factory=DAACStoreCache)
    _http: dict[str, HTTPStore] = field(default_factory=dict)
    _wrapped: dict[tuple[str, str], DiskCachingReadableStore] = field(
        default_factory=dict
    )
    _token: str | None = None
    _logged_in: bool = False
    _tracker: CacheSizeTracker | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.cache_dir is not None:
            self._tracker = CacheSizeTracker(
                self.cache_dir, max_bytes=self.cache_max_bytes
            )

    def _ensure_login(self) -> None:
        if not self._logged_in:
            earthaccess.login(strategy="netrc")
            self._logged_in = True
            token_dict = getattr(earthaccess.__auth__, "token", None) or {}
            self._token = (
                token_dict.get("access_token") if isinstance(token_dict, dict) else None
            )
            if not self._token:
                raise AuthUnavailable(
                    "earthaccess.login() did not produce a bearer token; check ~/.netrc"
                )

    def get_store(
        self, *, provider: str, url: str
    ) -> "S3Store | HTTPStore | DiskCachingReadableStore":
        """Return a store capable of reading `url` for the given CMR `provider`."""
        parsed = urlparse(url)
        if self.access == "direct":
            if parsed.scheme != "s3":
                raise AuthUnavailable(
                    f"--access direct expects s3:// URLs, got {url!r}. "
                    "The granules table likely has stale URLs from a previous "
                    "--access external sample. Delete output/survey.duckdb and "
                    "re-run sample."
                )
            # For S3 URLs the bucket is the netloc.
            bucket = parsed.netloc
            if not bucket:
                raise AuthUnavailable(f"cannot extract S3 bucket from url {url!r}")
            inner = self._s3.get_store(provider=provider, bucket=bucket)
            return self._maybe_wrap(("s3", bucket), inner, prefix=f"s3://{bucket}")

        if parsed.scheme not in ("http", "https"):
            raise AuthUnavailable(
                f"--access external expects http(s):// URLs, got {url!r}. "
                "The granules table likely has stale URLs from a previous "
                "--access direct sample. Delete output/survey.duckdb and "
                "re-run sample with --access external."
            )

        # external: HTTPStore per host.
        key = f"{parsed.scheme}://{parsed.netloc}"
        store = self._http.get(key)
        if store is None:
            self._ensure_login()
            from obstore.store import HTTPStore

            store = HTTPStore.from_url(
                key,
                client_options={
                    "default_headers": {"Authorization": f"Bearer {self._token}"},
                },
            )
            self._http[key] = store
        return self._maybe_wrap((parsed.scheme, parsed.netloc), store, prefix=key)

    def _maybe_wrap(
        self,
        wrap_key: tuple[str, str],
        inner,  # type: ignore[no-untyped-def]
        *,
        prefix: str,
    ):  # type: ignore[no-untyped-def]
        if self._tracker is None:
            return inner
        existing = self._wrapped.get(wrap_key)
        if existing is not None:
            return existing
        wrapped = DiskCachingReadableStore(inner, prefix=prefix, tracker=self._tracker)
        self._wrapped[wrap_key] = wrapped
        return wrapped
