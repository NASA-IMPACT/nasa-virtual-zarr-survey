"""Shared pytest fixtures."""
from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """A temporary DuckDB file path."""
    return tmp_path / "survey.duckdb"


@pytest.fixture
def tmp_results_dir(tmp_path: Path) -> Path:
    """A temporary results directory for partitioned Parquet writes."""
    d = tmp_path / "results"
    d.mkdir()
    return d
