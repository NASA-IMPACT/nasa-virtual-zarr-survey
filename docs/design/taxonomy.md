# Failure Taxonomy

The survey's `report` classifies each failing granule attempt into a `Bucket` (see `nasa_virtual_zarr_survey.taxonomy`). Buckets group failures so a reader can quickly see which classes of problem dominate and which collections to investigate.

Classification is a first-match-wins regex pipeline against the recorded `error_type` and `error_message`. New buckets and rules are added as real-world errors surface in the `OTHER` bucket of a pilot run.

## Buckets

| Bucket | Meaning | Example error string | Typical cause / next step |
|---|---|---|---|
| `SUCCESS` | Phase succeeded, not a failure bucket. | (no error) | Nothing to do. |
| `NO_PARSER` | VirtualiZarr has no parser registered for this format family. | `No VirtualiZarr parser registered for HDF4` | Affects HDF4 and GeoTIFF-without-virtual-tiff. Add a parser upstream or extend `dispatch_parser`. |
| `TIMEOUT` | The parse, dataset, or datatree phase didn't finish within the configured `--timeout` shared budget. | `dataset did not complete within 60s overall budget (elapsed 47.3s)` | Very large files, slow network, or parser-level hang. Increase `--timeout` or investigate the specific granule. |
| `FORBIDDEN` | HTTP 403 / AccessDenied on the granule URL. | `403 Forbidden`, `AccessDenied` | Running direct-S3 outside us-west-2, missing EULA acceptance, or an expired bearer token. |
| `AUTH_UNAVAILABLE` | `earthaccess.get_s3_credentials` returned no credentials for the DAAC. | `earthaccess returned no S3 credentials for provider 'X'` | Provider unknown to EDL's cloud-auth endpoint, or EDL login failed at session start. |
| `CANT_OPEN_FILE` | File bytes don't conform to the expected format. | `not a valid HDF5 file`, `signature of a valid netCDF` | Corrupted or truncated file, or the wrong parser was dispatched. |
| `UNDEFINED_FILL_VALUE` | VirtualiZarr couldn't read a required fill value from the file's metadata. | `Can't get fill value (fill value is undefined)` | Usually a VirtualiZarr bug or non-standard HDF5 encoding. File an issue with the `repro` script. |
| `AMBIGUOUS_ARRAY_TRUTH` | A library compared a numpy array in a boolean context without `.any()` / `.all()`. | `The truth value of an array with more than one element is ambiguous` | Almost always an upstream bug in the parser or xarray layer. File upstream. |
| `CONFLICTING_DIM_SIZES` | xarray couldn't flatten groups with mismatched dimensions into a single `Dataset`. | `conflicting sizes for dimension 'y': length 18557 on '1' and length 37114 on {'y': '0'}` | The file has hierarchical groups. Collections that fail Phase 4a with this bucket often succeed in Phase 4b (Datatreeability), which calls `to_virtual_datatree()` instead. |
| `GROUP_STRUCTURE` | NetCDF/HDF5 group structure couldn't be aligned across the sampled granules. | `not aligned with its parents`, `group structure` | Similar to `CONFLICTING_DIM_SIZES`. Hierarchical collection. |
| `DECODE_ERROR` | A codec or decoder failed on the raw chunk bytes. | `can only convert an array of size 1`, messages matching `decode` | Implementation gap in the codec or encoding metadata. |
| `VARIABLE_CHUNKS` | File uses variable-length or non-uniform chunks. | `variable length chunks not supported` | VirtualiZarr limitation, tracked as a feature request. |
| `UNSUPPORTED_CODEC` | Codec identified but not yet implemented. | `codec X not supported` | Add codec support in VirtualiZarr / numcodecs. |
| `UNSUPPORTED_FILTER` | HDF5 filter pipeline element not implemented. | `filter pipeline element not supported` | Add filter support in VirtualiZarr. |
| `SHARDING_UNSUPPORTED` | Zarr sharding encountered but not supported. | `sharding not supported` | Hypothesized; not yet observed in practice. |
| `NON_STANDARD_HDF5` | HDF5 feature outside the common spec surface. | (varies) | Hypothesized catch-all for esoteric HDF5 variants. |
| `COMPOUND_DTYPE` | File has compound (struct-valued) variables. | `compound dtype`, `compound type` | Zarr doesn't support compound dtypes directly; would require decomposition. |
| `STRING_DTYPE` | Variable-length string dtype encountered. | `dtype not supported: string`, `dtype.*string` | Variable-length strings need special handling in the Zarr v3 pipeline. |
| `NETWORK_ERROR` | Transport-level failure (connection reset, read timeout). | `ConnectionError`, `RemoteDisconnected` | Usually transient. Re-run to confirm. |
| `SAMPLE_INVALID` | The granule record was missing a URL or format family at attempt time. | `missing format family or data URL` | A sampling bug, not a VirtualiZarr issue. |
| `OTHER` | Uncategorized. | (varies) | See the "Top 20 Raw Errors in `OTHER`" section of the report, then refine `_RULES` in `src/nasa_virtual_zarr_survey/taxonomy.py`. |

## Adding a new bucket

When the `OTHER` bucket contains a recurring error, promote it to its own bucket:

1. Add a new member to `Bucket` in `src/nasa_virtual_zarr_survey/taxonomy.py`.
2. Add a `(type_regex, message_regex, bucket)` entry to `_RULES`. Order matters (first match wins). Be careful that the new rule doesn't shadow a more specific one above it.
3. Add a parametrized test case to `tests/unit/test_taxonomy.py::test_classify`.
4. Update the table above with the new bucket.
5. Re-run `report` (no need to re-run `attempt`) to pick up the updated classification.

## Applying rules to errors

`classify(error_type, error_message)` returns a single `Bucket`. Rules are evaluated top-to-bottom against the (type, message) pair. A rule matches when its non-None regex fields all match. The first match wins; otherwise the function returns `Bucket.OTHER`. Empty or `None` inputs for both fields return `Bucket.SUCCESS` (used by the reporting layer to short-circuit).
