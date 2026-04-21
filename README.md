# avacache SDKs

This repository contains two SDKs for the public Avalanche C-Chain parquet
archive at `https://parquet.avacache.com`:

- `python/` — PyPI package `avacache`
- `typescript/` — npm package `avacache`

Both clients target the same archive and keep the same core surface:

- fetch and cache `manifest.json`
- load one UTC day of `blocks`, `txs`, or `events`
- load a date range
- decode known hex-encoded numeric columns by default

There is no shared root workspace. Treat `python/` and `typescript/` as two
independent packages that happen to implement the same archive contract.

## What The Archive Contains

The public archive is organized around three daily parquet files per UTC date:

```text
manifest.json
daily/YYYY-MM-DD.blocks.parquet
daily/YYYY-MM-DD.txs.parquet
daily/YYYY-MM-DD.events.parquet
```

The manifest is authoritative. It tells clients:

- which dates exist
- the object key for each file
- the expected byte size and MD5
- the current schema version
- the declared columns and lookup files

Do not guess availability by constructing URLs alone. Read the manifest first if
you need a validated list of dates or integrity metadata.

For the full public archive contract, including manifest fields, lookup files,
schema tables, raw hex conventions, and versioning guarantees, see
[docs/archive-contract.md](docs/archive-contract.md).

## Quick Start

### Python

```python
from avacache import Client

with Client() as c:
    manifest = c.manifest()
    print(manifest.latest_complete_date)

    txs = c.load_day("2026-04-18", "txs")
    print(txs.num_rows)
```

### TypeScript

```ts
import { Client } from 'avacache';

const c = new Client();
const manifest = await c.manifest();
console.log(manifest.latest_complete_date);

const txs = await c.loadDay('2026-04-18', 'txs');
console.log(txs.length);
```

## Data Kinds And Decoding

Supported kinds:

- `blocks`
- `txs`
- `events`

Some numeric fields are stored upstream as hex strings. The SDKs decode them by
default on load:

- wei-scale fields such as `value` and `base_fee_per_gas`
- `events.log_index`

Decoded types differ by runtime:

- Python: wei fields become `DECIMAL(38,0)` in `pyarrow`, `log_index` becomes
  `int64`
- TypeScript: wei fields become `bigint`, `log_index` becomes `number`

Disable this with `decode_hex=False` in Python or `decodeHex: false` in
TypeScript if you want raw strings.

## Cache And Integrity

Both SDKs cache downloaded parquet locally by default.

- Python: `~/.cache/avacache/v1/<chain_id>/daily/`
- TypeScript in Node: `~/.cache/avacache/v1/<chain_id>/`
- TypeScript in browsers: IndexedDB (`avacache-v1-<chain_id>`)

Integrity behavior:

- Python verifies downloaded parquet against the manifest MD5 and size before
  moving it into cache.
- TypeScript in Node verifies MD5 and size.
- TypeScript in browsers verifies size only, because WebCrypto does not provide
  MD5.

The Python SDK also supports strict offline mode with `AVACACHE_OFFLINE=1`.

## Choosing The Right API

- Use `load_day` / `loadDay` for a single UTC date.
- Use `load_range` / `loadRange` for small to moderate spans you really want in
  one in-memory result.
- Use Python `iter_range()` for large spans; it yields one decoded day at a time
  and prefetches ahead.
- Use `url_for()` / `urlFor()` if another engine will read the parquet for you.
- Use the optional DuckDB helpers when you want SQL over the archive without
  learning the file layout.

Range semantics are intentionally forgiving: missing dates inside the requested
window are skipped, but the call fails if no files exist anywhere in the range.

## Docs

- Python package guide: [python/README.md](python/README.md)
- TypeScript package guide: [typescript/README.md](typescript/README.md)
- Shared archive contract: [docs/archive-contract.md](docs/archive-contract.md)
- Usage cookbook: [docs/cookbook.md](docs/cookbook.md)
- Lookup-file guide: [docs/lookups.md](docs/lookups.md)
- Contributor guide: [CONTRIBUTING.md](CONTRIBUTING.md)
