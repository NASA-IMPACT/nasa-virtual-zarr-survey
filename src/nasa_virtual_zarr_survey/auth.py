"""EDL login and per-DAAC S3 credential caching + obstore registry."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

import earthaccess


class AuthUnavailable(Exception):
    """Raised when earthaccess cannot mint S3 credentials for the given provider."""


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
        """Return a cached (or freshly-minted) obstore S3Store for this CMR provider.

        Raises AuthUnavailable if earthaccess returns empty credentials.
        """
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
        store = _build_store(creds, provider)
        self._entries[provider] = _Entry(store=store, minted_at=now)
        return store


def _build_store(creds: dict[str, str], provider: str) -> Any:
    """Construct an obstore S3Store from earthaccess credentials."""
    from obstore.store import S3Store

    return S3Store(
        bucket="",
        access_key_id=creds["accessKeyId"],
        secret_access_key=creds["secretAccessKey"],
        session_token=creds["sessionToken"],
        region="us-west-2",
    )
