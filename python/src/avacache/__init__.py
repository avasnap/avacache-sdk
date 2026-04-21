"""avacache — Python client for the public Avalanche C-Chain parquet archive."""

from avacache.client import Client, Kind
from avacache.manifest import Manifest, FileEntry


def __getattr__(name: str):
    # Lazy import so importing avacache does not require duckdb.
    if name in ("open_day", "open_range"):
        from avacache import duckdb_helper
        return getattr(duckdb_helper, name)
    raise AttributeError(f"module 'avacache' has no attribute {name!r}")


__all__ = ["Client", "Kind", "Manifest", "FileEntry", "open_day", "open_range"]
__version__ = "0.1.0"
