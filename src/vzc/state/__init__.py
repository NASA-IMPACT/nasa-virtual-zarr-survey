"""Survey state on disk: state.json, *.summary.json digests, and Parquet results.

Public API for the package re-exports the most commonly used helpers from
``_io``, ``_digest``, and ``_results``. Consumers should import from this
package, not from the underscore submodules.
"""

from vzc.state._digest import (
    LoadedSummary,
    dump_summary,
    load_summary,
)
from vzc.state._io import (
    SCHEMA_VERSION,
    AccessMode,
    CollectionRow,
    GranuleRow,
    SurveyState,
    delete_granules_for_collection,
    load_state,
    pending_granules,
    save_state,
    upsert_collections,
    upsert_granules,
)
from vzc.state._results import (
    attempted_pairs,
    count_rows,
    iter_rows,
    load_table,
    shard_paths,
)

__all__ = [
    "AccessMode",
    "CollectionRow",
    "GranuleRow",
    "LoadedSummary",
    "SCHEMA_VERSION",
    "SurveyState",
    "attempted_pairs",
    "count_rows",
    "delete_granules_for_collection",
    "dump_summary",
    "iter_rows",
    "load_state",
    "load_summary",
    "load_table",
    "pending_granules",
    "save_state",
    "shard_paths",
    "upsert_collections",
    "upsert_granules",
]
