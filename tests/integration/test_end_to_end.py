"""Live integration tests. Require EDL credentials in ~/.netrc.

Run with: uv run pytest -m integration -v
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def edl_available() -> bool:
    netrc = Path(os.path.expanduser("~/.netrc"))
    if not netrc.exists():
        pytest.skip("No ~/.netrc; integration tests need EDL credentials")
    return True


def test_pilot_three_collections(edl_available, tmp_path):
    """Run the full pipeline on 3 collections. Smoke-level assertion."""
    from nasa_virtual_zarr_survey.attempt import run_attempt
    from nasa_virtual_zarr_survey.discover import run_discover
    from nasa_virtual_zarr_survey.report import run_report
    from nasa_virtual_zarr_survey.sample import run_sample

    db = tmp_path / "s.duckdb"
    results = tmp_path / "r"
    out = tmp_path / "report.md"

    run_discover(db, limit=3)
    run_sample(db, n_bins=2)
    run_attempt(db, results, timeout_s=90)
    run_report(db, results, out)

    assert out.exists()
    assert "Verdicts" in out.read_text()
