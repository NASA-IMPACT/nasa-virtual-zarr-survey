"""Data shapes and tiny classifiers shared across the survey.

``core`` depends only on the standard library + pyarrow stubs; every other
subpackage may import from it.
"""

from vzc.core.formats import FormatFamily, classify_format
from vzc.core.processing_level import CUBE_MIN_RANK, parse_rank
from vzc.core.taxonomy import Bucket, classify
from vzc.core.types import (
    CoordInfo,
    Fingerprint,
    PendingGranule,
    SampleCollection,
    VarInfo,
    VerdictRow,
)

__all__ = [
    "Bucket",
    "CUBE_MIN_RANK",
    "CoordInfo",
    "Fingerprint",
    "FormatFamily",
    "PendingGranule",
    "SampleCollection",
    "VarInfo",
    "VerdictRow",
    "classify",
    "classify_format",
    "parse_rank",
]
