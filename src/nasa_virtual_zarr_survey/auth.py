"""EDL login and per-DAAC S3 credential caching + obstore registry."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

import earthaccess


class _Entry(NamedTuple):
    store: Any
    minted_at: datetime


@dataclass
class DAACStoreCache:
    """Caches an obstore S3Store per DAAC. Refreshes after TTL."""

    ttl: timedelta = timedelta(minutes=50)
    _logged_in: bool = False
    _entries: dict[str, _Entry] | None = None

    def __post_init__(self) -> None:
        if self._entries is None:
            self._entries = {}

    def _login(self) -> None:
        if not self._logged_in:
            earthaccess.login(strategy="netrc")
            self._logged_in = True

    def get_store(self, daac: str) -> Any:
        """Return a cached (or freshly-minted) obstore S3Store for this DAAC."""
        now = datetime.now(timezone.utc)
        entry = self._entries.get(daac)
        if entry and now - entry.minted_at < self.ttl:
            return entry.store
        self._login()
        creds = earthaccess.get_s3_credentials(daac=daac)
        store = _build_store(creds, daac)
        self._entries[daac] = _Entry(store=store, minted_at=now)
        return store


def _build_store(creds: dict[str, str], daac: str) -> Any:
    """Construct an obstore S3Store from earthaccess credentials."""
    from obstore.store import S3Store

    return S3Store(
        bucket="",  # bucket inferred from URL at read time
        access_key_id=creds["accessKeyId"],
        secret_access_key=creds["secretAccessKey"],
        session_token=creds["sessionToken"],
        region="us-west-2",
    )
