# SurveySession

A small adapter that lets the same code path serve `attempt` and `report` regardless of whether the source is a persistent `survey.duckdb` (the live pipeline) or a `config/locked_sample.json` (snapshot runs). Both modes expose the same `collections` and `granules` tables on a DuckDB connection.

::: nasa_virtual_zarr_survey.db_session.SurveySession
    handler: python
