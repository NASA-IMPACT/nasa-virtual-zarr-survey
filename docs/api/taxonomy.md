# Taxonomy

Empirically-derived error-class classifier. Maps `(error_type, error_message)` tuples to a `Bucket` value using an ordered list of regex rules. Bucket meanings, example errors, and triage steps are documented in [the taxonomy reference](../design/taxonomy.md). For instructions on adding a new bucket, see the [contributing guide](../contributing.md#refining-the-taxonomy).

::: nasa_virtual_zarr_survey.taxonomy.Bucket
    handler: python

::: nasa_virtual_zarr_survey.taxonomy.classify
    handler: python
