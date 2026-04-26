# Feature introductions

Reader for `config/feature_introductions.toml`, the hand-curated annotation file that drives feature markers on the funnel-over-time chart in the [Coverage over time](../results/history.md) page. Each entry pins a VirtualiZarr feature (e.g. `has_datatree`, `has_fits_parser`) to the date it shipped, the version it first appeared in, and the pipeline phase(s) it affects.

::: nasa_virtual_zarr_survey.feature_introductions.FeatureIntroduction
    handler: python

::: nasa_virtual_zarr_survey.feature_introductions.load_introductions
    handler: python

::: nasa_virtual_zarr_survey.feature_introductions.FeatureIntroductionsError
    handler: python
