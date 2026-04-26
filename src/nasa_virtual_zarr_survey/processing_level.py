"""Parse CMR ``ProcessingLevel.Id`` strings into ranks and apply survey thresholds.

CMR processing levels are free-text — observed values include ``"0"``, ``"1"``,
``"1A"``, ``"1B"``, ``"1C"``, ``"L1B"``, ``"2"``, ``"2A"``, ``"3"``, ``"4"``,
``"Not Provided"``, ``"NA"``, and ``""``. We collapse them to an integer 0-4
(stripping a leading ``L`` and trailing letter suffixes) for filter decisions.

Survey policy:

- Discover does not filter by processing level. Per-granule virtualization
  (parsability/datasetability) is processing-level-agnostic — an L1B HDF5
  granule loads as an ``xarray.Dataset`` just fine.
- ``CUBE_MIN_RANK = 3``: collections below L3 get cubability verdict
  ``EXCLUDED_BY_POLICY``, since L2 swath/orbital products are inherently not
  expected to combine into a single cube.
"""

from __future__ import annotations

import re

CUBE_MIN_RANK = 3

_LEADING_DIGIT = re.compile(r"^\s*L?(\d)")


def parse_rank(processing_level: str | None) -> int | None:
    """Return integer 0-4 for a CMR ProcessingLevel.Id, or None if unparsable."""
    if not processing_level:
        return None
    m = _LEADING_DIGIT.match(processing_level)
    if m is None:
        return None
    rank = int(m.group(1))
    if 0 <= rank <= 4:
        return rank
    return None
