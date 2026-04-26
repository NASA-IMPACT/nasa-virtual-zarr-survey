# Glossary

Terms used throughout this site, the report, and the design docs. Headings are alphabetized inside each group.

## NASA / CMR vocabulary

**CMR (Common Metadata Repository).** NASA's catalog of Earth-science data holdings. The survey starts from CMR and only considers entries marked `cloud_hosted=True`.

**Collection.** A logical dataset in CMR (one mission, one product). Identified by a *concept ID* like `C1996881146-POCLOUD`. The survey's verdict tables have one row per collection.

**Concept ID.** CMR's stable identifier. Collection IDs start with `C`, granule IDs start with `G`, both end with the provider short code (`-POCLOUD`, `-LPDAAC`, etc.).

**DAAC (Distributed Active Archive Center).** A NASA-funded archive that hosts a slice of EOSDIS holdings (PO.DAAC, GES DISC, LP DAAC, etc.). The "By DAAC" report table groups results by DAAC.

**EOSDIS (Earth Observing System Data and Information System).** Umbrella for NASA's Earth-science data system, including all DAACs. The survey filters CMR providers down to known EOSDIS provider codes.

**EDL (Earthdata Login).** NASA's SSO. The survey uses an EDL bearer token (`--access external`) or temporary S3 credentials minted via EDL (`--access direct`). Credentials live in `~/.netrc`.

**Granule.** A single data file in a collection (one swath, one tile, one timestamp). The survey samples up to N granules per collection across the temporal extent and runs phases 3, 4a, 4b on each.

**`usage_score`.** CMR's popularity metric per collection. `--top N` ranks collections by `usage_score` so the survey targets the most-used data first.

## Survey verdicts

**`all_pass`.** Every sampled granule for the collection passed this phase.

**`partial_pass`.** Some granules passed and some failed. Surfaces flakiness and granule-level heterogeneity.

**`all_fail`.** Every sampled granule failed this phase.

**`not_attempted`.** No applicable rows in the results log (typically because an upstream phase failed and the downstream one is gated on it).

**`skipped`.** The collection was filtered out before sampling because its declared format is not array-like (PDF, shapefile, CSV, etc.) and no parser would apply.

## Sampling

**Stratified sampling.** Granules picked evenly across the temporal extent (e.g. one per N equal-time bins). Default mode for the survey.

**Fallback sampling.** Used when a collection's temporal extent is missing or degenerate; falls back to a simple top-N pick.

**Unsampled.** A collection in the database that wasn't picked up by `sample` (e.g. zero granules returned by CMR). Shown for completeness in the Stratification table.

## VirtualiZarr concepts

**Codec.** A compression or filter pipeline element (zstd, blosc, gzip, shuffle, etc.). `UNSUPPORTED_CODEC` is a frequent failure bucket.

**Cubability.** Whether the per-granule virtual datasets in one collection share enough structure (same dims, same dtypes, monotonic concat axis) to be combined into a single virtual store. Phase 5; only attempted when Phase 4a is `all_pass`.

**DMR++.** OPeNDAP's serialized data-access metadata, a sidecar XML (typically `<file>.dmrpp` next to the data file in S3) that VirtualiZarr can use as a fast manifest source. The survey records `collections.has_cloud_opendap` (UMM-S association with cloud Hyrax) and `granules.dmrpp_granule_url` (constructed `https_url + ".dmrpp"`); use `--verify-dmrpp` on `sample` to HEAD-check each sidecar.

**Fingerprint.** A small JSON blob describing the structural shape of a virtual dataset (dims, dtypes, coordinate spans). Phase 5 compares fingerprints across granules of a collection.

**ManifestStore.** VirtualiZarr's intermediate representation: a Zarr-shaped store backed by a manifest of byte ranges into source files. Output of Phase 3.

**Parser.** Per-format reader (`HDFParser`, `NetCDF3Parser`, `FITSParser`, `DMRPPParser`, `ZarrParser`, `VirtualTIFF`) that produces a `ManifestStore` from a granule URL.

**Virtual dataset / virtual datatree.** An `xarray.Dataset` (Phase 4a) or `xarray.DataTree` (Phase 4b) backed by a `ManifestStore` instead of in-memory arrays. Reads pull byte ranges on demand.

## Failure-bucket reference

The taxonomy of failure buckets (`UNSUPPORTED_CODEC`, `CONFLICTING_DIM_SIZES`, `UNDEFINED_FILL_VALUE`, etc.) lives in [the taxonomy reference](design/taxonomy.md).
