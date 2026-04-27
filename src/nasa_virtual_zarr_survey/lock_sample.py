"""Write a `config/locked_sample.json` artifact from the current DuckDB state."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nasa_virtual_zarr_survey.db import connect, init_schema

LOCKED_SAMPLE_SCHEMA_VERSION = 3


def write_locked_sample(db_path: Path | str, out_path: Path | str) -> Path:
    """Read collections + granules from DuckDB and emit deterministic JSON.

    Output schema is the locked-sample format (schema_version 1):
    `{ schema_version, created_at, sampling_mode, collections[], granules[] }`.

    Granules carry both `s3_url` and `https_url` fields. The current DB stores
    only the URL for the access mode used at sample time (in `data_url`); the
    other column is populated only when the second access-mode pass has also
    been run. Either column may therefore be null in the JSON.
    """
    con = connect(db_path)
    init_schema(con)

    sampling_mode_row = con.execute(
        "SELECT value FROM run_meta WHERE key = 'sampling_mode'"
    ).fetchone()
    sampling_mode = sampling_mode_row[0] if sampling_mode_row else None

    collections = [
        {
            "concept_id": cid,
            "daac": daac,
            "provider": provider,
            "format_family": fam,
            "processing_level": level,
            "short_name": short,
            "version": ver,
        }
        for cid, daac, provider, fam, level, short, ver in con.execute(
            """
            SELECT concept_id, daac, provider, format_family, processing_level,
                   short_name, version
            FROM collections
            WHERE skip_reason IS NULL
            ORDER BY concept_id
            """
        ).fetchall()
    ]

    granules: list[dict[str, Any]] = []
    for (
        collection_id,
        granule_id,
        data_url,
        https_url,
        bin_,
        n_total,
        size,
        access_mode,
    ) in con.execute(
        """
        SELECT collection_concept_id, granule_concept_id, data_url,
               https_url, stratification_bin, n_total_at_sample, size_bytes, access_mode
        FROM granules
        ORDER BY collection_concept_id, stratification_bin, granule_concept_id
        """
    ).fetchall():
        s3_url: str | None
        ext_url: str | None
        if access_mode == "direct":
            s3_url = data_url
            ext_url = https_url
        else:
            s3_url = None
            ext_url = data_url
        granules.append(
            {
                "collection_concept_id": collection_id,
                "granule_concept_id": granule_id,
                "s3_url": s3_url,
                "https_url": ext_url,
                "stratification_bin": bin_,
                "n_total_at_sample": n_total,
                "size_bytes": size,
            }
        )

    payload = {
        "schema_version": LOCKED_SAMPLE_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "sampling_mode": sampling_mode,
        "collections": collections,
        "granules": granules,
    }

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    return out
