"""nasa-virtual-zarr-survey: measure VirtualiZarr compatibility across NASA CMR."""

try:
    from importlib.metadata import version
    __version__ = version("nasa-virtual-zarr-survey")
except Exception:
    __version__ = "0.1.0"
