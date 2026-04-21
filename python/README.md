# avacache

Python SDK for the public Avalanche C-Chain parquet archive at
`https://parquet.avacache.com`.

It downloads daily parquet files on demand, verifies them against the published
manifest, caches them locally, and returns `pyarrow.Table` objects with known
hex-encoded numeric columns decoded by default.

## Install

```bash
pip install avacache
pip install 'avacache[duckdb]'
pip install 'avacache[pandas]'
pip install 'avacache[polars]'
pip install 'avacache[notebook]'
pip install 'avacache[all]'
```

Extras:

- `duckdb` adds `open_day()` and `open_range()`
- `pandas` and `polars` install the common dataframe integrations people
  typically use after loading `pyarrow.Table`
- `notebook` adds `tqdm` and `ipython` so range loads can show progress bars in
  notebooks

## Quick Start

```python
from avacache import Client

with Client() as c:
    m = c.manifest()
    print(m.latest_complete_date)

    txs = c.load_day("2026-04-18", "txs")
    print(txs.schema)

    blocks = c.load_range("2026-04-16", "2026-04-18", "blocks")
    print(blocks.num_rows)
```

Valid kinds are:

- `"blocks"`
- `"txs"`
- `"events"`

## What `load_day()` Returns

`load_day()` and `load_range()` return `pyarrow.Table`.

By default the client decodes the known hex-encoded numeric columns:

- wei-scale fields: `value`, `gas_price`, `effective_gas_price`,
  `max_priority_fee_per_gas`, `base_fee_per_gas` -> `DECIMAL(38,0)`
- event log index: `log_index` -> `int64`

Hashes, addresses, topics, and calldata-like fields stay as strings.

Raw-value edge cases:

- `"0x"` decodes to numeric zero
- `""` decodes to `None`
- invalid hex values also become `None`

Disable decoding if you want the raw archive values:

```python
from avacache import Client

c = Client(decode_hex=False)
raw = c.load_day("2026-04-18", "txs")
```

## Client API

```python
from avacache import Client

c = Client(
    chain_id=43114,
    base_url="https://parquet.avacache.com",
    cache_dir="~/.cache/avacache/v1",
    offline=False,
    timeout=60.0,
    decode_hex=True,
)
```

Important constructor options:

- `chain_id`: defaults to `43114`
- `base_url`: archive root; also configurable via `AVACACHE_BASE_URL`
- `cache_dir`: cache root; also configurable via `AVACACHE_CACHE_DIR`
- `offline`: if true, refuse network and serve only from local cache
- `timeout`: `httpx` timeout in seconds
- `decode_hex`: decode known numeric hex columns on load

Useful methods:

- `manifest(force=False) -> Manifest`
- `available_dates(kind) -> list[date]`
- `latest_complete_date() -> date | None`
- `url_for(date, kind) -> str`
- `load_day(date, kind) -> pyarrow.Table`
- `iter_range(start, end, kind, concurrency=4, progress=None)`
- `load_range(start, end, kind, concurrency=8, progress=None) -> pyarrow.Table`
- `prune_cache(max_gb=50.0) -> int`

Range semantics:

- dates are inclusive
- `end < start` raises `ValueError`
- missing dates inside the range are skipped
- the call fails only if no parquet files exist anywhere in the requested range

The archive schema, manifest fields, lookup files, and versioning guarantees
are documented in [docs/archive-contract.md](../docs/archive-contract.md).
Additional examples live in [docs/cookbook.md](../docs/cookbook.md), and
lookup-file usage is covered in [docs/lookups.md](../docs/lookups.md).

## Common Workflows

Load the latest complete day without guessing:

```python
from avacache import Client

with Client() as c:
    latest = c.latest_complete_date()
    if latest is None:
        raise RuntimeError("archive has no complete dates yet")

    txs = c.load_day(latest, "txs")
    print(latest, txs.num_rows)
```

Stream a long range one day at a time instead of materializing everything in
memory:

```python
from avacache import Client

with Client() as c:
    for day, events in c.iter_range(
        "2026-04-01",
        "2026-04-30",
        "events",
        concurrency=4,
        progress=True,
    ):
        print(day, events.num_rows)
```

Warm the cache online, then reopen in strict offline mode:

```python
from avacache import Client

cache_dir = "/tmp/avacache-docs"

with Client(cache_dir=cache_dir, offline=False) as online:
    online.load_day("2026-04-18", "blocks")
    online.load_day("2026-04-18", "txs")

with Client(cache_dir=cache_dir, offline=True) as offline:
    txs = offline.load_day("2026-04-18", "txs")
    print(txs.num_rows)
```

Refresh the manifest if you are polling for newly completed days:

```python
from avacache import Client

with Client() as c:
    fresh_manifest = c.manifest(force=True)
    print(fresh_manifest.generated_at)
```

`manifest()` is memoized in-process for 5 minutes by default. Parquet cache
reuse persists across client instances as long as they point at the same cache
directory.

## Single-Day, Range, And Streaming Workflows

For small queries, use `load_day()` or `load_range()`:

```python
from avacache import Client

with Client() as c:
    day = c.load_day("2026-04-18", "events")
    week = c.load_range("2026-04-12", "2026-04-18", "events")
```

For longer spans, prefer `iter_range()` so you do not hold every day in memory
at once:

```python
from avacache import Client

with Client() as c:
    for day, txs in c.iter_range(
        "2026-04-01",
        "2026-04-18",
        "txs",
        concurrency=4,
        progress=True,
    ):
        print(day, txs.num_rows)
```

`iter_range()` prefetches ahead using a thread pool, but yields tables in date
order. `progress=None` auto-enables notebook progress bars when the optional
dependencies are installed.

## Cache And Offline Mode

By default files are cached under:

```text
~/.cache/avacache/v1/<chain_id>/
```

Daily parquet objects are stored under `daily/` and keyed by the manifest MD5,
so if the upstream archive rebuilds a date, the new object lands at a different
cache path and the stale file is not reused accidentally.

The client also caches `manifest.json`, which is what makes offline mode work:

```bash
export AVACACHE_OFFLINE=1
```

Or in code:

```python
from avacache import Client

c = Client(offline=True)
```

In offline mode:

- cached manifests and parquet files are served normally
- missing cached content raises immediately
- no network calls are attempted

To trim disk usage:

```python
from avacache import Client

removed = Client().prune_cache(max_gb=20)
print(f"removed {removed} files")
```

`prune_cache()` removes the least-recently-accessed parquet files until the
cache is under budget.

## DuckDB Helpers

Install the extra first:

```bash
pip install 'avacache[duckdb]'
```

Then use:

```python
from avacache import open_day, open_range

with open_day("2026-04-18") as con:
    failed = con.execute(
        "SELECT COUNT(*) FROM txs WHERE status = 0"
    ).fetchone()[0]
    print(failed)

with open_range("2026-04-01", "2026-04-07") as con:
    out = con.execute("""
        SELECT block_number, COUNT(*) AS n
        FROM events
        GROUP BY block_number
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
```

Important behavior:

- `open_day()` eagerly loads all three daily tables before registering views
- `open_range()` eagerly materializes the full range for all three kinds before
  opening DuckDB
- both helpers accept `client=` if you want to reuse a preconfigured offline or
  cache-tuned client

The helpers register three views:

- `blocks`
- `txs`
- `events`

They load data through the `Client`, so decoded numeric columns are already in
their Arrow types when DuckDB sees them.

## Failure Modes

Common exceptions to expect:

- `FileNotFoundError` when a requested day or whole range is absent from the
  manifest
- `RuntimeError` when `offline=True` and required cache files are missing
- `ValueError` on MD5 mismatch while downloading parquet
- `httpx` transport exceptions on network failures

## Environment Variables

- `AVACACHE_BASE_URL` — override the archive host
- `AVACACHE_CACHE_DIR` — override the local cache root
- `AVACACHE_OFFLINE=1` — serve only from local cache
