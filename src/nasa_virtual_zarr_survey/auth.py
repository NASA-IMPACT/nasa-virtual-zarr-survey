"""EDL login + per-provider/per-hostname store cache (S3 or HTTPS)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, NamedTuple
from urllib.parse import urlparse

import earthaccess


class AuthUnavailable(Exception):
    """Raised when credentials cannot be minted for a given provider."""


class _Entry(NamedTuple):
    store: Any
    minted_at: datetime


@dataclass
class DAACStoreCache:
    """Caches an obstore S3Store per CMR provider. Refreshes after TTL."""

    ttl: timedelta = timedelta(minutes=50)
    _logged_in: bool = False
    _entries: dict[str, _Entry] = field(default_factory=dict)

    def _login(self) -> None:
        if not self._logged_in:
            earthaccess.login(strategy="netrc")
            self._logged_in = True

    def get_store(self, provider: str) -> Any:
        """Return a cached (or freshly-minted) obstore S3Store for this CMR provider."""
        now = datetime.now(timezone.utc)
        entry = self._entries.get(provider)
        if entry and now - entry.minted_at < self.ttl:
            return entry.store
        self._login()
        creds = earthaccess.get_s3_credentials(provider=provider)
        if not creds or "accessKeyId" not in creds:
            raise AuthUnavailable(
                f"earthaccess returned no S3 credentials for provider {provider!r}"
            )
        store = _build_s3_store(creds, provider)
        self._entries[provider] = _Entry(store=store, minted_at=now)
        return store


def _build_s3_store(creds: dict[str, str], provider: str) -> Any:
    from obstore.store import S3Store

    return S3Store(
        bucket="",
        access_key_id=creds["accessKeyId"],
        secret_access_key=creds["secretAccessKey"],
        session_token=creds["sessionToken"],
        region="us-west-2",
    )


# Keep old name for backwards compat with any existing monkeypatches in tests.
_build_store = _build_s3_store


@dataclass
class StoreCache:
    """Unified cache that dispatches on access mode ('direct' for S3, 'external' for HTTPS)."""

    access: Literal["direct", "external"] = "direct"
    _s3: DAACStoreCache = field(default_factory=DAACStoreCache)
    _http: dict[str, Any] = field(default_factory=dict)
    _token: str | None = None
    _logged_in: bool = False

    def _ensure_login(self) -> None:
        if not self._logged_in:
            earthaccess.login(strategy="netrc")
            self._logged_in = True
            token_dict = getattr(earthaccess.__auth__, "token", None) or {}
            self._token = token_dict.get("access_token") if isinstance(token_dict, dict) else None
            if not self._token:
                raise AuthUnavailable(
                    "earthaccess.login() did not produce a bearer token; check ~/.netrc"
                )

    def get_store(self, *, provider: str, url: str) -> Any:
        """Return a store capable of reading `url` for the given CMR `provider`."""
        if self.access == "direct":
            return self._s3.get_store(provider)

        parsed = urlparse(url)
        key = f"{parsed.scheme}://{parsed.netloc}"
        store = self._http.get(key)
        if store is not None:
            return store
        self._ensure_login()
        from obspec_utils.stores import AiohttpStore

        store = AiohttpStore(
            base_url=key,
            headers={"Authorization": f"Bearer {self._token}"},
        )
        self._http[key] = store
        return store
