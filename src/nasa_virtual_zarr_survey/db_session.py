"""Source abstraction: adapts a DuckDB file or a locked-sample JSON into the
same `collections` and `granules` query surface used by `attempt` and `report`."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import duckdb

from nasa_virtual_zarr_survey.db import connect, init_schema

AccessMode = Literal["direct", "external"]


class SurveySession:
    """Owns a DuckDB connection that exposes `collections` and `granules` as
    queryable tables, sourced from either a persistent .duckdb file or a
    locked-sample JSON loaded into an in-memory database."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    @classmethod
    def from_duckdb(cls, path: Path | str) -> "SurveySession":
        con = connect(path)
        init_schema(con)
        return cls(con)

    @classmethod
    def from_locked_sample(
        cls,
        path: Path | str,
        *,
        access: AccessMode,
    ) -> "SurveySession":
        data = json.loads(Path(path).read_text())
        if data.get("schema_version") != 1:
            raise ValueError(
                f"Unsupported locked_sample schema_version: "
                f"{data.get('schema_version')!r}"
            )

        con = duckdb.connect(":memory:")
        init_schema(con)
        now = datetime.now(timezone.utc)

        for c in data["collections"]:
            con.execute(
                """
                INSERT INTO collections
                (concept_id, daac, format_family, processing_level,
                 short_name, version, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    c["concept_id"],
                    c.get("daac"),
                    c.get("format_family"),
                    c.get("processing_level"),
                    c.get("short_name"),
                    c.get("version"),
                    now,
                ],
            )

        url_field = "s3_url" if access == "direct" else "https_url"
        for g in data["granules"]:
            url = g.get(url_field)
            if url is None:
                continue
            con.execute(
                """
                INSERT INTO granules
                (collection_concept_id, granule_concept_id, data_url,
                 https_url, temporal_bin, size_bytes, stratified,
                 access_mode, sampled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    g["collection_concept_id"],
                    g["granule_concept_id"],
                    url,
                    g.get("https_url"),
                    g.get("temporal_bin"),
                    g.get("size_bytes"),
                    g.get("stratified", False),
                    access,
                    now,
                ],
            )

        return cls(con)
