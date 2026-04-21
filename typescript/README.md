# avacache (TypeScript)

Isomorphic TypeScript client for the public Avalanche C-Chain parquet
archive — runs in Node 18+ and modern browsers.

```ts
import { Client } from 'avacache';

const c = new Client();                                // parquet.avacache.com
const m = await c.manifest();
m.latest_complete_date; // "2026-04-18"

const rows = await c.loadDay('2026-04-18', 'txs');     // Row[]
const range = await c.loadRange('2026-04-01', '2026-04-18', 'txs');
c.urlFor('2026-04-18', 'txs');
```

Hex-encoded numeric columns are decoded at load:

- `value`, `gas_price`, `effective_gas_price`, `max_priority_fee_per_gas`,
  `base_fee_per_gas` → `bigint`
- `log_index` → `number`
- Hashes, addresses, topics, and call data remain as `string`

Disable with `new Client({ decodeHex: false })`.

## Cache

- **Node**: `~/.cache/avacache/v1/<chain_id>/` (override with
  `AVACACHE_CACHE_DIR` or `cacheDir`).
- **Browser**: IndexedDB (`avacache-v1-<chain_id>`).

Pass `cache: false` to disable; pass a custom `Cache` adapter for SSR.

## Optional DuckDB helper

```ts
import { openDay } from 'avacache/duckdb';
const con = await openDay('2026-04-18');
const out = await con.query('SELECT COUNT(*) FROM txs WHERE status = 0');
```

Requires `@duckdb/duckdb-wasm` as a peer dep.
