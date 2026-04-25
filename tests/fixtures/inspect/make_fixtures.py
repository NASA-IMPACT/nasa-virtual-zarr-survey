"""Generate tiny test fixtures for inspector tests.

Run with: uv run python tests/fixtures/inspect/make_fixtures.py

Idempotent: re-running overwrites the existing fixtures.
"""

from __future__ import annotations

import shutil
from pathlib import Path

HERE = Path(__file__).parent


DMRPP_XML = """<?xml version="1.0" encoding="ISO-8859-1"?>
<Dataset xmlns="http://xml.opendap.org/ns/DAP/4.0#"
         xmlns:dmrpp="http://xml.opendap.org/dap/dmrpp/1.0.0#"
         name="example">
  <Float32 name="temp">
    <Dim name="time" size="3"/>
    <dmrpp:chunks compressionType="deflate">
      <dmrpp:chunk offset="0" nBytes="12"/>
    </dmrpp:chunks>
  </Float32>
</Dataset>
"""


def make_hdf5() -> None:
    import h5py
    import numpy as np

    p = HERE / "two_groups.h5"
    with h5py.File(p, "w") as f:
        sci = f.create_group("science")
        ds = sci.create_dataset(
            "temp",
            data=np.arange(12, dtype="float32").reshape(3, 4),
            chunks=(3, 4),
            compression="gzip",
        )
        ds.attrs["units"] = "K"
        meta = f.create_group("metadata")
        meta.create_dataset("flag", data=np.zeros(3, dtype="uint8"))


def make_zarr() -> None:
    import numpy as np
    import zarr

    p = HERE / "simple.zarr"
    if p.exists():
        shutil.rmtree(p)
    store = zarr.storage.LocalStore(str(p))
    root = zarr.create_group(store=store, zarr_format=3)
    a = root.create_array("temp", shape=(3, 4), chunks=(3, 4), dtype="float32")
    a[:] = np.arange(12, dtype="float32").reshape(3, 4)


def make_netcdf3() -> None:
    from scipy.io import netcdf_file

    p = HERE / "classic.nc"
    with netcdf_file(str(p), "w", version=1) as f:
        f.createDimension("time", 5)
        v = f.createVariable("temp", "f", ("time",))
        v[:] = [1.0, 2.0, 3.0, 4.0, 5.0]
        v.units = "K"


def make_geotiff() -> None:
    import numpy as np
    import tifffile

    p = HERE / "tiled.tif"
    arr = np.arange(64 * 64, dtype="uint16").reshape(64, 64)
    tifffile.imwrite(str(p), arr, tile=(32, 32), compression="zlib", predictor=2)


def make_dmrpp() -> None:
    (HERE / "example.dmrpp").write_text(DMRPP_XML)


def make_fits() -> None:
    import numpy as np
    from astropy.io import fits

    p = HERE / "two_hdus.fits"
    primary = fits.PrimaryHDU()
    img = fits.ImageHDU(data=np.arange(16, dtype="float32").reshape(4, 4), name="IMG")
    fits.HDUList([primary, img]).writeto(str(p), overwrite=True)


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    make_hdf5()
    make_zarr()
    make_netcdf3()
    make_geotiff()
    make_dmrpp()
    make_fits()


if __name__ == "__main__":
    main()
