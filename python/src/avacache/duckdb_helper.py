"""DuckDB helper — open one (or more) days of the archive as SQL views.

Mirrors the internal `cchain_cache.open_day` but fetches through the
public archive client so hex columns come back already decoded.

    from avacache import open_day
    with open_day("2026-04-18") as con:
        n = con.execute("SELECT COUNT(*) FROM txs WHERE status=0").fetchone()[0]

Views registered: `blocks`, `txs`, `events`.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Iterator

from avacache.client import Client


@contextmanager
def open_day(
    d: str | date,
    *,
    client: Client | None = None,
) -> Iterator["duckdb.DuckDBPyConnection"]:
    try:
        import duckdb
    except ImportError as e:
        raise ImportError(
            "open_day() requires DuckDB. Install with: pip install 'avacache[duckdb]'"
        ) from e

    c = client or Client()
    blocks = c.load_day(d, "blocks")
    txs = c.load_day(d, "txs")
    events = c.load_day(d, "events")

    con = duckdb.connect()
    con.register("blocks", blocks)
    con.register("txs", txs)
    con.register("events", events)
    try:
        yield con
    finally:
        con.close()


@contextmanager
def open_range(
    start: str | date,
    end: str | date,
    *,
    client: Client | None = None,
) -> Iterator["duckdb.DuckDBPyConnection"]:
    """Open a date range as concatenated views (blocks, txs, events)."""
    try:
        import duckdb
    except ImportError as e:
        raise ImportError(
            "open_range() requires DuckDB. Install with: pip install 'avacache[duckdb]'"
        ) from e

    c = client or Client()
    blocks = c.load_range(start, end, "blocks")
    txs = c.load_range(start, end, "txs")
    events = c.load_range(start, end, "events")

    con = duckdb.connect()
    con.register("blocks", blocks)
    con.register("txs", txs)
    con.register("events", events)
    try:
        yield con
    finally:
        con.close()
