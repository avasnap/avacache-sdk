---
name: avacache
description: Read the public Avalanche C-Chain parquet archive at https://parquet.avacache.com — daily blocks / transactions / event-log parquet files plus function-selector and event-topic JSON lookups, addressed through an authoritative `manifest.json`. Covers the archive contract (layout, schemas, hex conventions, integrity) and three supported access modes — the bundled `avacache` CLI (zero-code shell use), the `avacache` SDK (Python on PyPI / TypeScript on npm), or driving downloads directly with any HTTP client and reading the raw parquet with DuckDB / Polars / Spark / pyarrow / hyparquet. Trigger when the user wants Avalanche on-chain data by date without running an RPC node, mentions "avacache", "C-Chain parquet", "Avalanche archive", `manifest.json`, `load_day`/`loadDay`, `load_range`/`loadRange`, `iter_range`, `function_selectors.json`, `event_topics.json`, or asks how to query historical Avalanche blocks/txs/events at scale.
---

# avacache

Public read-only parquet archive of the Avalanche C-Chain (`chain_id = 43114`), with a manifest-based contract and two SDKs over it. Both modes — SDK and direct parquet — are first-class.

## Archive contract (read this first)

### Layout

```
https://parquet.avacache.com/
├── manifest.json
├── daily/
│   ├── YYYY-MM-DD.blocks.parquet
│   ├── YYYY-MM-DD.txs.parquet
│   └── YYYY-MM-DD.events.parquet
└── lookups/
    ├── function_selectors.json
    └── event_topics.json
```

Dates are **UTC `YYYY-MM-DD`**. A "complete day" has all three parquet kinds. Don't probe URLs — read the manifest.

### `manifest.json` is authoritative

```json
{
  "chain_id": 43114,
  "generated_at": "2026-04-21T17:45:00Z",
  "schema_version": "v2",
  "latest_complete_date": "2026-04-20",
  "columns": { "blocks": [...], "txs": [...], "events": [...] },
  "lookups": {
    "function_selectors": { "key": "lookups/function_selectors.json", "size": 123, "md5": "..." },
    "event_topics":       { "key": "lookups/event_topics.json",       "size": 456, "md5": "..." }
  },
  "files": [
    { "date": "2026-04-20", "kind": "txs",
      "key": "daily/2026-04-20.txs.parquet",
      "size": 123456, "md5": "abc...", "schema_version": "v2" }
  ]
}
```

Hard rules:

- Build URLs as `base_url + "/" + entry.key`. Never construct `daily/<date>.<kind>.parquet` from scratch.
- Verify downloads against the entry's `size` and `md5` (MD5 is over the raw object body, not pretty-printed JSON).
- A `(date, kind)` may be **rebuilt** in place — same `key`, new `md5`/`size`. Cache keys must include `md5`.
- `latest_complete_date` ≠ newest file seen; it's the newest date with **all three** kinds.
- `schema_version` bumps on column add/remove/rename/retype. Adding new dates or new lookup files does *not* bump it.

### Schemas

#### `blocks` (one row per block)

| Column | Type | Notes |
|---|---|---|
| `number` | int64 | block number |
| `timestamp` | int64 | unix seconds, UTC |
| `gas_used` | int64 | |
| `gas_limit` | int64 | |
| `base_fee_per_gas` | string (hex wei) | null pre-EIP-1559 |

#### `txs` (one row per transaction)

| Column | Type | Notes |
|---|---|---|
| `block_number` | int64 | join → `blocks.number` |
| `tx_hash` | string | lowercase `0x…` |
| `from_address` | string | lowercase |
| `to_address` | string \| null | null = contract creation |
| `value` | string (hex wei) | |
| `gas_used` | int64 | |
| `effective_gas_price` | string (hex wei) | |
| `gas_price` | string \| null (hex wei) | legacy |
| `max_priority_fee_per_gas` | string \| null (hex wei) | EIP-1559 tip |
| `type` | int64 | 0 legacy / 1 access list / 2 EIP-1559 |
| `status` | int64 | 1 = success, 0 = failed |
| `input_prefix` | string | `"0x" + 4-byte selector`, or `""` for value-only transfers |

#### `events` (one row per log)

| Column | Type | Notes |
|---|---|---|
| `block_number` | int64 | |
| `tx_hash` | string | join → `txs.tx_hash` |
| `log_index` | string (hex) | per-block log index |
| `address` | string | emitting contract |
| `topic0` | string | event signature hash |
| `topic1`/`topic2`/`topic3` | string \| null | indexed args |
| `data` | string (hex) | non-indexed event data |

### Raw hex conventions (when reading parquet directly)

- Wei-scale integers (`value`, `gas_price`, `effective_gas_price`, `max_priority_fee_per_gas`, `base_fee_per_gas`) are lowercase `0x…` hex strings.
- `events.log_index` is also hex upstream.
- `"0x"` ⇒ numeric **zero**.
- `""` ⇒ **null-like** (treat as missing).
- Invalid hex ⇒ null.
- `null` is just missing.

The SDKs decode these on load. Direct parquet readers see raw values.

### Lookup files

- `lookups/function_selectors.json` — keys are lowercase 4-byte selectors (`"0xa9059cbb"`); each value is a **list of candidates** `[{ signature, count, abi }, ...]` because 4-byte selectors collide. Sort by `count` desc to pick the most common signature. Join on `txs.input_prefix`.
- `lookups/event_topics.json` — same shape, keyed by `topic0` (keccak256 of the canonical signature, so collisions are rare in practice — but the value is still a list). Join on `events.topic0`.

`txs.input_prefix == ""` (value-only transfer) won't resolve. Missing keys are normal — treat as "unresolved", not corruption. Verify the JSON body bytes against `manifest.lookups[*].size` / `md5` exactly like parquet entries. (Note: `docs/lookups.md` shows a single-object value shape — that's stale; the live archive returns a list.)

### Joins

- `blocks.number` ↔ `txs.block_number`
- `txs.tx_hash` ↔ `events.tx_hash` (preferred for row-level)
- `txs.input_prefix` ↔ `function_selectors.json`
- `events.topic0` ↔ `event_topics.json`
- `txs.status`: 1 = success, 0 = failed.

## Mode A0 — `avacache` CLI (zero-code)

Both SDKs ship an `avacache` binary that mirrors the SDK surface 1:1. Same subcommands in both languages — pick whichever runtime is already installed. **The CLI contract is documented here, not in the source; it can change between minor versions, so re-read this section before scripting against it.**

```
avacache manifest [--json] [--force]            # summary or full manifest JSON
avacache latest                                 # prints latest_complete_date (exit 1 if none)
avacache dates [--kind blocks|txs|events]       # one date per line
avacache url <date> <kind>                      # verified parquet URL (exit 2 if not in manifest)
avacache fetch <date> <kind> [--out PATH]       # SDK-verified download
                                                #   Python: prints local cache path, copies to --out if given
                                                #   TS:     --out PATH is REQUIRED
avacache show <date> <kind> [--limit N]         # load + print rows (default limit 20; --limit 0 = all)
                            [--format table|ndjson|json|csv]
avacache lookup selector <0x4byte> [--json]     # resolves via function_selectors.json (large; streamed in TS)
avacache lookup topic    <0xtopic0> [--json]    # resolves via event_topics.json
```

Global flags accepted before any subcommand: `--base-url`, `--cache-dir`, `--chain-id`, `--no-decode-hex`. Python additionally has `--offline`.

Output formatting:

- `show` defaults to `table` on a TTY, `ndjson` when piped — safe for `| jq`, `| awk`, etc.
- `lookup` without `--json` prints `signature\tcount` lines, sorted by count desc (most common signature first). With `--json` it prints the full candidate list including `abi`.
- All other commands print plain text suitable for shell capture.

Composing with other tools:

```bash
# Latest day's events, the 5 highest-count event topics, as JSON
avacache show "$(avacache latest)" events --format ndjson --limit 0 \
  | jq -r .topic0 | sort | uniq -c | sort -rn | head -5

# Hand off to DuckDB without local download (Mode B bridge)
duckdb -c "SELECT count(*) FROM read_parquet('$(avacache url 2026-05-08 txs)') WHERE status = 0"

# Warm the cache once, then run analyses offline (Python only)
avacache fetch 2026-05-08 txs >/dev/null
avacache --offline show 2026-05-08 txs --limit 5
```

Install:

- Python: `pip install avacache` exposes `avacache` via `[project.scripts]`. Also runs as `python -m avacache`.
- TypeScript: `npm install -g avacache` for global, or `npx avacache <cmd>`. Node-only; the browser bundle does not include the CLI.

## Mode A — Use the SDK (managed cache + decoded values)

### Install

- Python: `pip install avacache` (extras: `[duckdb]`, `[pandas]`, `[polars]`, `[notebook]`, `[all]`)
- TypeScript: `npm install avacache` (peer dep `@duckdb/duckdb-wasm` only if you use the `avacache/duckdb` entry)

### Hard rules for the SDK

1. Use `latest_complete_date()` / `latestCompleteDate()` or `available_dates(kind)` / `availableDates(kind)`. Don't pass "today".
2. Three kinds only: `"blocks"`, `"txs"`, `"events"`.
3. Hex decoding is **on by default**. Wei → `DECIMAL(38,0)` (Py) / `bigint` (TS); `events.log_index` → `int64` / `number`. Pass `decode_hex=False` / `decodeHex: false` only when the caller wants raw strings.
4. Range loading is inclusive, **skips** missing inner dates, fails only when *no* dates in the window exist. Python `iter_range()` streams day-by-day with prefetch — use it for multi-month `txs`/`events`. TS only has serial `loadRange` today; for big spans loop `availableDates` + `loadDay` instead.
5. Cache integrity: Python and TS-on-Node verify MD5 + size; TS-in-browser verifies size only (WebCrypto has no MD5). Cache root: `AVACACHE_CACHE_DIR`, `cache_dir=` (Py), `cacheDir:` (TS Node only — browser is fixed to IndexedDB `avacache-v1-<chain_id>`).
6. **Offline mode is Python-only**: `AVACACHE_OFFLINE=1` or `Client(offline=True)`. Don't claim TS has it.
7. DuckDB helpers are optional. Py: `[duckdb]` extra → `open_day` / `open_range`. TS: install `@duckdb/duckdb-wasm` → `import { openDay } from 'avacache/duckdb'`. Never import the DuckDB entry from core.

### Recipes

```python
# Python — latest complete day
from avacache import Client
with Client() as c:
    latest = c.latest_complete_date()
    if latest is None: raise RuntimeError("no complete date yet")
    txs = c.load_day(latest, "txs")  # pyarrow.Table, hex decoded

# Python — large range, streaming
with Client() as c:
    for day, txs in c.iter_range("2026-04-01", "2026-04-30", "txs", concurrency=4):
        ...
```

```ts
// TypeScript — latest complete day
import { Client } from 'avacache';
const c = new Client();
const latest = await c.latestCompleteDate();
if (!latest) throw new Error('no complete date yet');
const txs = await c.loadDay(latest, 'txs');  // Array<Record<string, unknown>>
```

| Need | Python | TypeScript |
|---|---|---|
| One day | `load_day` | `loadDay` |
| Small range, in memory | `load_range` | `loadRange` |
| Big range, streaming | `iter_range` | manual `availableDates` + `loadDay` loop |
| SQL over the day(s) | `open_day`/`open_range` (extra) | `openDay` from `'avacache/duckdb'` |
| Verified URL for another engine | `url_for` | `urlFor` |
| Force-refresh manifest | `c.manifest(force=True)` | `c.manifest(true)` |
| Trim disk cache | `prune_cache` | (manual) |

## Mode B — Drive the archive directly (your own client → raw parquet)

Use this when you want to scan parquet from DuckDB, Polars, Spark, ClickHouse, pyarrow, hyparquet, or anything else — with or without the SDK in the loop.

### Two sub-flavors

**B1. SDK-warmed, then read from disk.** Run the SDK once to populate the cache (it verifies MD5/size for you), then point your engine at the cached parquet files. The cache layout is documented and stable per chain:

- Python: `~/.cache/avacache/v1/<chain_id>/daily/<date>.<kind>.<md5[:16]>.parquet`
- TS-Node: `~/.cache/avacache/v1/<chain_id>/<entry.key with "/" → "__" and "|" + entry.md5 with "|" → "--">` (e.g. `daily__2026-04-18.txs.parquet--<md5>`)
- TS-browser: IndexedDB `avacache-v1-<chain_id>` (not file-addressable; use Mode A or B2 in browsers)

`Client.url_for(date, kind)` / `urlFor` returns the canonical remote URL after confirming availability. Combine with the local cache path if your engine wants a local file.

**B2. No SDK at all.** Do the manifest dance yourself. Reference implementation:

```python
import hashlib, json, urllib.request

BASE = "https://parquet.avacache.com"

def get(url: str) -> bytes:
    with urllib.request.urlopen(url) as r:
        return r.read()

manifest = json.loads(get(f"{BASE}/manifest.json"))
entry = next(f for f in manifest["files"]
             if f["date"] == manifest["latest_complete_date"] and f["kind"] == "txs")

body = get(f"{BASE}/{entry['key']}")
assert len(body) == entry["size"], "size mismatch"
assert hashlib.md5(body).hexdigest() == entry["md5"], "md5 mismatch"

# Now hand `body` (or write it to disk) to any parquet reader.
import pyarrow.parquet as pq, io
table = pq.read_table(io.BytesIO(body))
```

DuckDB over a verified URL (after the manifest check above):

```sql
-- DuckDB will stream the parquet via httpfs; no local cache needed
SELECT count(*) FROM read_parquet('https://parquet.avacache.com/daily/2026-04-20.txs.parquet')
WHERE status = 0;
```

```python
# DuckDB + lookup join, raw hex preserved
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("""
  CREATE VIEW txs    AS SELECT * FROM read_parquet('https://parquet.avacache.com/daily/2026-04-20.txs.parquet');
  CREATE VIEW events AS SELECT * FROM read_parquet('https://parquet.avacache.com/daily/2026-04-20.events.parquet');
""")
```

When operating in Mode B, **remember the raw conventions** from the contract section: wei is hex strings, `"0x"` is zero, `""` is null-like, `events.log_index` is hex. Decode in your engine (e.g. DuckDB `try_cast` after stripping `0x`, or apply `int(x, 16)` in pandas) — the SDKs are the only thing that decodes for you.

### When to pick which

| Situation | Mode |
|---|---|
| Agent shell command, one-off lookup | **A0 (CLI)** |
| Quick script, single language, modest data | A (SDK) |
| Notebook / repeatable analysis with offline reruns | A — Python with `offline=True` |
| Multi-month aggregation, want SQL | B with DuckDB (or A with `open_range`) |
| Already a Spark / Polars / ClickHouse pipeline | B — point it at verified URLs |
| Browser app | A (TS) — uses IndexedDB |
| Non-Python/TS runtime (Go, Rust, etc.) | B2 — manifest + verify is ~30 lines |

## SDK editing parity rule

Both SDKs implement the same three-layer pipeline (manifest → fetch+verify → decode). When changing the **client surface** (`manifest`, `load_day`/`loadDay`, `load_range`/`loadRange`, hex opt-out) or the hex column lists in `hex.py` / `hex.ts`, mirror in the other SDK unless there is a deliberate reason not to. See [AGENTS.md](../../../AGENTS.md).

## Authoritative references (load these for non-trivial work)

- [README.md](../../../README.md) — overview
- [docs/archive-contract.md](../../../docs/archive-contract.md) — manifest fields, schemas, hex conventions, versioning rules
- [docs/cookbook.md](../../../docs/cookbook.md) — recipes (gap detection, joins, failed-tx queries, offline warmup, manifest refresh)
- [docs/lookups.md](../../../docs/lookups.md) — verified-fetch pattern with full Py + TS examples
- [python/README.md](../../../python/README.md) — full Python API
- [typescript/README.md](../../../typescript/README.md) — full TS API
