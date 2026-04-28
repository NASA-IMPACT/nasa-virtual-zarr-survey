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
    from vzc.pipeline._attempt import _run_attempt
    from vzc.db_session import SurveySession
    from vzc.cmr._discover import discover
    from vzc.render import render
    from vzc.cmr._sample import sample

    state_path = tmp_path / "state.json"
    results = tmp_path / "r"
    out = tmp_path / "report.md"

    discover(state_path, limit=3)
    sample(state_path, n_bins=2)
    session = SurveySession.from_state_path(state_path, access="direct")
    _run_attempt(session, results, timeout_s=90, skip_override_validation=True)
    render(session, results, out)

    assert out.exists()
    text = out.read_text()
    assert "## Overview" in text
    assert "Per-collection verdicts" in text
