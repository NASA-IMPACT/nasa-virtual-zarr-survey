# Snapshot

Run one survey snapshot against the currently-prepared environment. The snapshot's date and (for previews) git overrides are read from the same `pyproject.toml` that pinned the env, so a typical run is just `nasa-virtual-zarr-survey snapshot` after editing `[tool.uv]` and running `uv lock`.

The CLI entry point is `nasa-virtual-zarr-survey snapshot`. `run_snapshot` orchestrates `attempt` + `report --no-render --export ...` and copies the active `uv.lock` beside the digest for release snapshots.

::: nasa_virtual_zarr_survey.snapshot.run_snapshot
    handler: python

::: nasa_virtual_zarr_survey.snapshot.read_pyproject_exclude_newer
    handler: python

::: nasa_virtual_zarr_survey.snapshot.read_pyproject_git_sources
    handler: python

::: nasa_virtual_zarr_survey.snapshot.SnapshotError
    handler: python
