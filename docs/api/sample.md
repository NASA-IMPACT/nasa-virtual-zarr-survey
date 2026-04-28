# sample

Phase 2: pick N granules per collection, stratified across positional
offsets in CMR's `revision_date` ordering. Records both `s3_url`
(`--access direct`) and `https_url` (`--access external`) per granule,
so downstream phases can flip access modes without re-sampling.

For collections discovered with `has_cloud_opendap=True`, each sampled
granule's `dmrpp_granule_url` is recorded as `https_url + ".dmrpp"`.

::: vzc.sample
    handler: python
