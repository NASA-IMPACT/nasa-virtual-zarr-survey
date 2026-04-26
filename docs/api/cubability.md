# Cubability

Per-granule fingerprint extraction and metadata-only check of whether a collection's granules could be combined into a coherent virtual store. Runs as Phase 5 inside `report`, gated on Phase 4a (`dataset_verdict == 'all_pass'`).

`extract_fingerprint` reads structural shape (dims, dtypes, coordinate spans) from a virtual dataset; `check_cubability` compares fingerprints across granules and returns a `CubabilityVerdict` (`FEASIBLE`, `INCOMPATIBLE`, `INCONCLUSIVE`, `NOT_ATTEMPTED`, `EXCLUDED_BY_POLICY`). Fingerprints are persisted in the Parquet log via `fingerprint_to_json` / `fingerprint_from_json`.

::: nasa_virtual_zarr_survey.cubability.CubabilityVerdict
    handler: python

::: nasa_virtual_zarr_survey.cubability.CubabilityResult
    handler: python

::: nasa_virtual_zarr_survey.cubability.extract_fingerprint
    handler: python

::: nasa_virtual_zarr_survey.cubability.check_cubability
    handler: python

::: nasa_virtual_zarr_survey.cubability.fingerprint_to_json
    handler: python

::: nasa_virtual_zarr_survey.cubability.fingerprint_from_json
    handler: python
