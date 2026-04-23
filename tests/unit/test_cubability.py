from nasa_virtual_zarr_survey.cubability import (
    CubabilityVerdict,
    check_cubability,
    fingerprint_from_json,
    fingerprint_to_json,
)


def _fp(
    time_size=10,
    time_hash="aaa",
    time_min=0,
    time_max=9,
    lat_hash="lat",
    dtype="float32",
    codecs=("Blosc",),
):
    return {
        "dims": {"time": time_size, "lat": 5, "lon": 10},
        "data_vars": {
            "temp": {
                "dtype": dtype,
                "dims": ["time", "lat", "lon"],
                "chunks": [1, 5, 10],
                "fill_value": None,
                "codecs": list(codecs),
            }
        },
        "coords": {
            "time": {
                "dtype": "int64",
                "dims": ["time"],
                "shape": [time_size],
                "values_hash": time_hash,
                "min": time_min,
                "max": time_max,
            },
            "lat": {
                "dtype": "float32",
                "dims": ["lat"],
                "shape": [5],
                "values_hash": lat_hash,
                "min": -45.0,
                "max": 45.0,
            },
            "lon": {
                "dtype": "float32",
                "dims": ["lon"],
                "shape": [10],
                "values_hash": "lon",
                "min": -90.0,
                "max": 90.0,
            },
        },
    }


def test_feasible_concat_on_time():
    fps = [
        _fp(time_hash="a", time_min=0, time_max=9),
        _fp(time_hash="b", time_min=10, time_max=19),
        _fp(time_hash="c", time_min=20, time_max=29),
    ]
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.FEASIBLE
    assert r.concat_dim == "time"
    assert r.reason == ""


def test_incompatible_variable_names():
    fps = [_fp(), _fp()]
    fps[1]["data_vars"]["EXTRA"] = fps[1]["data_vars"]["temp"]
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.INCOMPATIBLE
    assert "variables differ" in r.reason


def test_incompatible_dtype():
    fps = [
        _fp(time_hash="a", time_min=0, time_max=9),
        _fp(time_hash="b", time_min=10, time_max=19, dtype="float64"),
    ]
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.INCOMPATIBLE
    assert "dtype" in r.reason


def test_incompatible_codecs():
    fps = [
        _fp(time_hash="a", time_min=0, time_max=9),
        _fp(time_hash="b", time_min=10, time_max=19, codecs=("Zstd",)),
    ]
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.INCOMPATIBLE
    assert "codecs" in r.reason


def test_incompatible_non_concat_coord():
    # time_size varies (making time the unambiguous concat dim by size),
    # but lat_hash differs, which should fail as a non-concat coord mismatch.
    fps = [
        _fp(time_size=10, time_hash="a", time_min=0, time_max=9),
        _fp(
            time_size=20, time_hash="b", time_min=10, time_max=19, lat_hash="DIFFERENT"
        ),
    ]
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.INCOMPATIBLE
    assert "lat" in r.reason


def test_incompatible_concat_coord_overlap():
    fps = [
        _fp(time_hash="a", time_min=0, time_max=9),
        _fp(time_hash="b", time_min=5, time_max=15),
    ]
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.INCOMPATIBLE
    assert "overlaps or reverses" in r.reason


def test_inconclusive_no_varying_dim():
    fps = [_fp(), _fp()]  # identical
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.INCONCLUSIVE
    assert "cannot identify" in r.reason


def test_inconclusive_ambiguous_concat_dim():
    fps = [
        _fp(time_hash="a", time_min=0, time_max=9),
        _fp(time_hash="b", time_min=10, time_max=19, lat_hash="differentlat"),
    ]
    r = check_cubability(fps)
    assert r.verdict is CubabilityVerdict.INCONCLUSIVE
    assert "ambiguous concat dim" in r.reason


def test_inconclusive_too_few_fingerprints():
    r = check_cubability([_fp()])
    assert r.verdict is CubabilityVerdict.INCONCLUSIVE


def test_fingerprint_json_roundtrip():
    fp = _fp()
    s = fingerprint_to_json(fp)
    restored = fingerprint_from_json(s)
    assert restored == fp
