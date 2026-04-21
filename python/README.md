# avacache

Python client for the public Avalanche C-Chain parquet archive.

```python
from avacache import Client

c = Client()                                       # defaults to parquet.avacache.com
c.manifest().latest_complete_date                  # "2026-04-18"
t = c.load_day("2026-04-18", "txs")                # pyarrow.Table
r = c.load_range("2026-04-01", "2026-04-18", "txs")
```

Daily parquet files are downloaded on demand, MD5-verified, and cached to
`~/.cache/avacache/v1/<chain_id>/daily/`.

Wei columns (`value`, `gas_price`, `effective_gas_price`,
`max_priority_fee_per_gas`, `base_fee_per_gas`) are decoded from hex strings
to `DECIMAL(38,0)`. Event `log_index` is decoded to `int64`. Hashes and
addresses are preserved as strings. Disable with `Client(decode_hex=False)`.

## Environment

- `AVACACHE_BASE_URL` — override the archive host
- `AVACACHE_CACHE_DIR` — override the local cache path
- `AVACACHE_OFFLINE=1` — refuse network, serve only from local cache

## Install

```
pip install avacache               # core (pyarrow)
pip install 'avacache[duckdb]'     # + DuckDB helper
pip install 'avacache[pandas]'     # + .to_pandas() friendly
pip install 'avacache[polars]'     # + polars
pip install 'avacache[all]'
```
