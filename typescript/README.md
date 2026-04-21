# avacache (TypeScript)

Isomorphic TypeScript SDK for the public Avalanche C-Chain parquet archive at
`https://parquet.avacache.com`.

It runs in Node 18+ and modern browsers, fetches and validates archive objects,
caches them automatically, and returns plain JavaScript row objects.

## Install

```bash
npm install avacache
```

Optional DuckDB support:

```bash
npm install avacache @duckdb/duckdb-wasm
```

## Quick Start

```ts
import { Client } from 'avacache';

const c = new Client();

const manifest = await c.manifest();
console.log(manifest.latest_complete_date);

const txs = await c.loadDay('2026-04-18', 'txs');
console.log(txs.length);

const blocks = await c.loadRange('2026-04-16', '2026-04-18', 'blocks');
console.log(blocks.length);
```

Valid kinds are:

- `'blocks'`
- `'txs'`
- `'events'`

## What `loadDay()` Returns

`loadDay()` and `loadRange()` return `Row[]`, where `Row` is a plain
`Record<string, unknown>`.

Known hex-encoded numeric fields are decoded in place by default:

- `value`, `gas_price`, `effective_gas_price`, `max_priority_fee_per_gas`,
  `base_fee_per_gas` -> `bigint`
- `log_index` -> `number`

Hashes, addresses, topics, and calldata-like fields remain strings.

Disable decoding globally:

```ts
const c = new Client({ decodeHex: false });
```

Or per call:

```ts
const rows = await c.loadDay('2026-04-18', 'txs', { decodeHex: false });
```

## Client API

```ts
import { Client } from 'avacache';

const c = new Client({
  chainId: 43114,
  baseUrl: 'https://parquet.avacache.com',
  cacheDir: '/tmp/avacache',
  decodeHex: true,
  cache: undefined,
  fetch: globalThis.fetch,
});
```

Important options:

- `chainId`: defaults to `43114`
- `baseUrl`: archive root
- `cacheDir`: Node-only cache root override
- `decodeHex`: decode known numeric hex columns on load
- `cache`: custom cache adapter, or `false` to disable caching
- `fetch`: custom fetch implementation for SSR, testing, or nonstandard runtimes

Useful methods:

- `manifest(force?)`
- `availableDates(kind)`
- `latestCompleteDate()`
- `urlFor(date, kind)`
- `loadDay(date, kind, opts?)`
- `loadRange(start, end, kind, opts?)`

Range semantics:

- dates are inclusive
- `end < start` throws
- missing dates inside the range are skipped
- the call fails only if no parquet files exist anywhere in the requested range

The archive schema, manifest fields, lookup files, and versioning guarantees
are documented in [docs/archive-contract.md](../docs/archive-contract.md).
Additional examples live in [docs/cookbook.md](../docs/cookbook.md), and
lookup-file usage is covered in [docs/lookups.md](../docs/lookups.md).

## Runtime And Recipes

Node batch load with filesystem cache:

```ts
import { Client } from 'avacache';

const c = new Client({ cacheDir: '/tmp/avacache-ts' });
const latest = await c.latestCompleteDate();
if (!latest) throw new Error('archive has no complete dates yet');

const txs = await c.loadDay(latest, 'txs');
console.log(latest, txs.length);
```

Browser app with IndexedDB-backed cache and decoded values:

```ts
import { Client } from 'avacache';

const c = new Client();
const rows = await c.loadDay('2026-04-18', 'blocks');
const baseFee = rows[0]?.base_fee_per_gas;

if (typeof baseFee === 'bigint' || baseFee === null) {
  console.log(baseFee);
}
```

SSR or worker runtime with custom `fetch` and custom cache:

```ts
import { Client, type Cache } from 'avacache';

const memory = new Map<string, Uint8Array>();

const cache: Cache = {
  async get(key) {
    return memory.get(key) ?? null;
  },
  async put(key, bytes) {
    memory.set(key, bytes);
  },
};

const c = new Client({
  fetch: globalThis.fetch,
  cache,
});

const manifest = await c.manifest(true);
console.log(manifest.generated_at);
```

DuckDB flow for SQL over remote parquet:

```ts
import { openDay } from 'avacache/duckdb';

const con = await openDay('2026-04-18');
const out = await con.query(
  'SELECT COUNT(*) AS failed FROM txs WHERE status = 0',
);
console.log(out.toArray());
await con.close();
```

Important behavior:

- in Node, the default cache is local filesystem
- in browsers, the default cache is IndexedDB
- in other runtimes, the client falls back to `NoopCache` unless you provide a
  custom cache
- `manifest()` is memoized in-process for 5 minutes; pass `manifest(true)` to
  force a refresh
- decode mutates loaded rows in place, and only happens in `loadDay()` and
  `loadRange()`
- `avacache/duckdb` scans remote parquet URLs directly, so it does not use SDK
  cache or SDK decode logic

## Runtime Behavior

### Node

In Node, the default cache lives under:

```text
~/.cache/avacache/v1/<chain_id>/
```

Downloaded objects are verified against both size and MD5 before being cached.

`cacheDir` and `AVACACHE_CACHE_DIR` both override the cache root.

Cache entries are keyed by file key plus manifest MD5, so rebuilt days naturally
land at a different cache key.

### Browser

In browsers, the default cache is IndexedDB:

```text
avacache-v1-<chain_id>
```

Objects are verified by size only. Browsers do not expose MD5 through
WebCrypto, so exact MD5 verification is intentionally skipped there.

## Custom Cache Adapters

The package exports the `Cache` interface and `NoopCache`.

Disable caching entirely:

```ts
const c = new Client({ cache: false });
```

Provide a custom adapter for SSR or framework-managed storage:

```ts
import { Client, type Cache } from 'avacache';

const memoryCache: Cache = {
  async get(key) {
    return store.get(key) ?? null;
  },
  async put(key, bytes) {
    store.set(key, bytes);
  },
};

const c = new Client({ cache: memoryCache });
```

## DuckDB-WASM Helper

The optional helper lives at `avacache/duckdb`:

```ts
import { openDay } from 'avacache/duckdb';

const con = await openDay('2026-04-18');
const out = await con.query(
  'SELECT COUNT(*) AS failed FROM txs WHERE status = 0',
);
console.log(out.toArray());
```

This helper is intentionally split into a separate entry point so the core SDK
does not force `@duckdb/duckdb-wasm` into every bundle.

## Notes And Limits

- `loadRange()` currently loads days serially and returns one large in-memory
  array.
- The SDK focuses on daily parquet objects; if you need `manifest.lookups`,
  read the manifest and fetch those JSON objects directly.
- `urlFor()` is a simple path builder. Use `manifest()` first if you need to
  validate that a date exists before handing the URL to another tool.
