"""Error-class taxonomy for virtualization attempts.

Seeded from titiler-cmr-compatibility's `IncompatibilityReason` plus hypotheses
for VirtualiZarr-specific failure modes. Refined after the 50-collection pilot
by reading raw `error_message` values under the `OTHER` bucket.
"""
from __future__ import annotations

import re
from enum import StrEnum


class Bucket(StrEnum):
    SUCCESS = "SUCCESS"
    NO_PARSER = "NO_PARSER"
    TIMEOUT = "TIMEOUT"
    FORBIDDEN = "FORBIDDEN"
    CANT_OPEN_FILE = "CANT_OPEN_FILE"
    GROUP_STRUCTURE = "GROUP_STRUCTURE"
    DECODE_ERROR = "DECODE_ERROR"
    VARIABLE_CHUNKS = "VARIABLE_CHUNKS"
    UNSUPPORTED_CODEC = "UNSUPPORTED_CODEC"
    UNSUPPORTED_FILTER = "UNSUPPORTED_FILTER"
    SHARDING_UNSUPPORTED = "SHARDING_UNSUPPORTED"
    NON_STANDARD_HDF5 = "NON_STANDARD_HDF5"
    COMPOUND_DTYPE = "COMPOUND_DTYPE"
    STRING_DTYPE = "STRING_DTYPE"
    NETWORK_ERROR = "NETWORK_ERROR"
    SAMPLE_INVALID = "SAMPLE_INVALID"
    OTHER = "OTHER"


# Ordered rules: first match wins. Each is (error_type_regex, message_regex, bucket).
# Pass None for a field to skip matching it.
_RULES: list[tuple[re.Pattern | None, re.Pattern | None, Bucket]] = [
    (re.compile(r"NoParserAvailable"), None, Bucket.NO_PARSER),
    (re.compile(r"SampleInvalid"), None, Bucket.SAMPLE_INVALID),
    (re.compile(r"TimeoutError"), None, Bucket.TIMEOUT),
    (None, re.compile(r"403|Forbidden|Unauthorized|AccessDenied", re.I), Bucket.FORBIDDEN),
    (None, re.compile(r"variable[- ]length chunks|variable chunks", re.I), Bucket.VARIABLE_CHUNKS),
    (None, re.compile(r"codec.*(not supported|unknown|unsupported)", re.I), Bucket.UNSUPPORTED_CODEC),
    (None, re.compile(r"filter.*(not supported|unknown|unsupported)|filter pipeline element not supported", re.I), Bucket.UNSUPPORTED_FILTER),
    (None, re.compile(r"sharding.*not supported", re.I), Bucket.SHARDING_UNSUPPORTED),
    (None, re.compile(r"compound.*dtype|compound type", re.I), Bucket.COMPOUND_DTYPE),
    (None, re.compile(r"string.*dtype|dtype.*string", re.I), Bucket.STRING_DTYPE),
    (None, re.compile(r"not aligned with its parents|group structure", re.I), Bucket.GROUP_STRUCTURE),
    (None, re.compile(r"not a valid netCDF|valid HDF5|signature of", re.I), Bucket.CANT_OPEN_FILE),
    (None, re.compile(r"decode|can only convert an array of size 1", re.I), Bucket.DECODE_ERROR),
    (None, re.compile(r"ConnectionError|RemoteDisconnected|timed out reading", re.I), Bucket.NETWORK_ERROR),
]


def classify(error_type: str | None, error_message: str | None) -> Bucket:
    """Classify an attempt into a bucket. None/None => SUCCESS."""
    if error_type is None and error_message is None:
        return Bucket.SUCCESS
    etype = error_type or ""
    emsg = error_message or ""
    for type_re, msg_re, bucket in _RULES:
        if type_re and not type_re.search(etype):
            continue
        if msg_re and not msg_re.search(emsg):
            continue
        return bucket
    return Bucket.OTHER
