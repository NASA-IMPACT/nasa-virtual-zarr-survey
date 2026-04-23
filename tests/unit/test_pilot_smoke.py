from click.testing import CliRunner

from nasa_virtual_zarr_survey.__main__ import cli


def test_pilot_dry_run(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.discover.run_discover", lambda *a, **k: 50
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.sample.run_sample", lambda *a, **k: 100
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.attempt.run_attempt", lambda *a, **k: 100
    )
    monkeypatch.setattr(
        "nasa_virtual_zarr_survey.report.run_report", lambda *a, **k: None
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "pilot",
            "--db",
            str(tmp_path / "s.duckdb"),
            "--results",
            str(tmp_path / "r"),
            "--out",
            str(tmp_path / "report.md"),
            "--sample",
            "50",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Pilot complete" in result.output
