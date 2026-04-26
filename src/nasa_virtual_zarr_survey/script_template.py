"""Shared snippet emitters for ``repro``- and ``probe``-generated scripts.

Pure string-rendering functions. No I/O, no state. Both
``repro.generate_script`` and ``probe.generate_script`` consume these so the
two CLI surfaces emit identical login / store / cache / inspect blocks.

Each renderer returns a multi-line snippet whose lines start at column 4
(inside ``def main()``), so callers can splice the output directly into the
body of a generated ``main`` function without further indentation.
"""

from __future__ import annotations

from urllib.parse import urlparse

from nasa_virtual_zarr_survey.formats import FormatFamily


def _registry_key(url: str) -> str:
    """Return the ``scheme://netloc`` prefix used as the ObjectStoreRegistry key."""
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    return f"{scheme}://{netloc}"


def _s3_bucket(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc


def render_cache_argparse() -> str:
    """Render the ``--cache``/``--cache-dir``/``--cache-max-size`` argparse block.

    Always emitted — the generated script accepts cache flags whether or not
    the operator opts in at runtime. Lines are indented 4 spaces (inside
    ``main``).
    """
    return (
        "    import argparse\n"
        "    from pathlib import Path\n"
        "\n"
        "    parser_cli = argparse.ArgumentParser(description=__doc__)\n"
        '    parser_cli.add_argument("--cache", action="store_true", '
        'help="enable on-disk granule cache")\n'
        '    parser_cli.add_argument("--cache-dir", type=Path, '
        'default=Path.home() / ".cache" / "nasa-virtual-zarr-survey")\n'
        '    parser_cli.add_argument("--cache-max-size", type=int, '
        'default=50 * 1024**3, help="bytes")\n'
        "    args = parser_cli.parse_args()\n"
    )


def render_earthaccess_login() -> str:
    """Render the bare ``earthaccess.login(strategy="netrc")`` line."""
    return '    earthaccess.login(strategy="netrc")\n'


def render_login_and_store(*, url: str, provider: str | None, registry_key: str) -> str:
    """Render earthaccess login + obstore store construction.

    Dispatches on URL scheme:

    - ``s3://...`` → ``S3Store`` with ``earthaccess.get_s3_credentials``
    - ``https://...`` → ``HTTPStore.from_url`` with the EDL bearer token

    Other schemes raise ``ValueError`` — the survey only supports S3/HTTPS
    granules, and a generated script that silently produced a TODO would mask
    the real problem (a malformed URL bound at gen time).
    """
    return render_earthaccess_login() + render_store(
        url=url, provider=provider, registry_key=registry_key
    )


def render_store(*, url: str, provider: str | None, registry_key: str) -> str:
    """Render only the obstore store construction (no login).

    Useful when the login call is emitted earlier (e.g. so a UMM dump can
    happen between login and store construction).
    """
    if url.startswith("s3://"):
        bucket = _s3_bucket(url)
        return (
            f"    creds = earthaccess.get_s3_credentials(provider={provider!r})\n"
            "    from obstore.store import S3Store\n"
            "    store = S3Store(\n"
            f"        bucket={bucket!r},\n"
            '        access_key_id=creds["accessKeyId"],\n'
            '        secret_access_key=creds["secretAccessKey"],\n'
            '        session_token=creds["sessionToken"],\n'
            '        region="us-west-2",\n'
            "    )\n"
        )
    if url.startswith("https://") or url.startswith("http://"):
        return (
            '    token_dict = getattr(earthaccess.__auth__, "token", None) or {}\n'
            '    token = token_dict.get("access_token")\n'
            "    from obstore.store import HTTPStore\n"
            "    store = HTTPStore.from_url(\n"
            f"        {registry_key!r},\n"
            '        client_options={"default_headers": '
            '{"Authorization": f"Bearer {token}"}},\n'
            "    )\n"
        )
    raise ValueError(
        f"unsupported URL scheme for store construction: {url!r} "
        "(expected s3:// or https://)"
    )


def render_cache_wiring(*, registry_key: str) -> str:
    """Render the ``if args.cache: ...`` block that wraps ``store`` on disk."""
    return (
        "    if args.cache:\n"
        "        from nasa_virtual_zarr_survey.cache import (\n"
        "            CacheSizeTracker,\n"
        "            DiskCachingReadableStore,\n"
        "        )\n"
        "\n"
        "        tracker = CacheSizeTracker(args.cache_dir, "
        "max_bytes=args.cache_max_size)\n"
        "        store = DiskCachingReadableStore(\n"
        f"            store, prefix={registry_key!r}, tracker=tracker\n"
        "        )\n"
    )


def render_inspect_block(*, url: str, family: FormatFamily) -> str:
    """Render ``family = ...`` plus the ``inspect_url(...)`` call.

    The format literal is baked at gen time so the operator can edit it.
    Caller is responsible for emitting the surrounding section comment.
    """
    return (
        f"    url = {url!r}\n"
        f'    family = FormatFamily("{family.value}")\n'
        "    inspect_url(url=url, family=family, store=store)\n"
    )
