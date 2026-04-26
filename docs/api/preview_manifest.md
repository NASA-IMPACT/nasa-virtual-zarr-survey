# Preview manifest

Parser and validator for `config/snapshot_previews/<date>-<label>.toml`. Preview manifests are the curated alternative to ad-hoc `[tool.uv.sources]` edits: they capture the snapshot date, label, optional description, and one or more git overrides (URL + hex SHA) in a committed file.

::: nasa_virtual_zarr_survey.preview_manifest.PreviewManifest
    handler: python

::: nasa_virtual_zarr_survey.preview_manifest.load_manifest
    handler: python

::: nasa_virtual_zarr_survey.preview_manifest.PreviewManifestError
    handler: python
