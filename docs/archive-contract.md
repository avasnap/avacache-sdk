# Archive Contract

This document describes the public data contract served by
`https://parquet.avacache.com` and consumed by both SDKs in this repository.

If you are writing anything more advanced than `load_day()` against a known
date, use this document together with the SDK READMEs. The SDKs are thin
clients over this archive shape, not a separate storage format.

## Layout

```text
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

Rules that matter to consumers:

- dates are UTC days in `YYYY-MM-DD` format
- a complete day has all three parquet kinds present
- `manifest.json` is authoritative for availability, size, MD5, and schema
- do not assume a URL exists just because the date string looks valid

## Manifest-First Workflow

Every client flow should start from `manifest.json`:

1. Fetch the manifest.
2. Read `latest_complete_date` or `files[]` to determine availability.
3. Find the matching `(date, kind)` entry.
4. Use that entry's `key`, `size`, `md5`, and `schema_version`.

That is the safe way to:

- load the latest complete day
- enumerate available dates
- verify a direct parquet download
- notice rebuilds where the same date gets a new `md5` or `size`

## `manifest.json`

Representative shape:

```json
{
  "chain_id": 43114,
  "generated_at": "2026-04-21T17:45:00Z",
  "schema_version": "v2",
  "latest_complete_date": "2026-04-20",
  "columns": {
    "blocks": ["number", "timestamp", "gas_used", "gas_limit", "base_fee_per_gas"],
    "txs": ["block_number", "tx_hash", "from_address", "to_address", "value"],
    "events": ["block_number", "tx_hash", "log_index", "address", "topic0"]
  },
  "lookups": {
    "function_selectors": {
      "key": "lookups/function_selectors.json",
      "size": 123,
      "md5": "..."
    }
  },
  "files": [
    {
      "date": "2026-04-20",
      "kind": "txs",
      "key": "daily/2026-04-20.txs.parquet",
      "size": 123456,
      "md5": "abc...",
      "schema_version": "v2"
    }
  ]
}
```

Field meanings:

| Field | Meaning |
|---|---|
| `chain_id` | Current archive chain ID. Today this is Avalanche C-Chain `43114`. |
| `generated_at` | When the manifest was produced, in UTC ISO-8601 with `Z`. |
| `schema_version` | Public contract version for the archive. |
| `latest_complete_date` | Latest UTC date where all three daily kinds are present. |
| `columns` | Declared column order for each kind. |
| `lookups` | Static JSON reference files with their keys, sizes, and MD5s. |
| `files` | One entry per `(date, kind)` parquet object. |

Important semantics:

- `latest_complete_date` is about completeness, not just the newest file seen.
- `columns` is the public schema declaration clients should trust.
- `files[]` is append-only by date/kind identity, but an existing entry's `md5`
  and `size` may change if that date is rebuilt.
- each file entry also carries its own `schema_version`

## Daily Schemas

### `blocks`

One row per block.

| Column | Type | Meaning |
|---|---|---|
| `number` | int64 | Block number. |
| `timestamp` | int64 | Unix seconds, UTC. |
| `gas_used` | int64 | Total gas consumed in the block. |
| `gas_limit` | int64 | Block gas limit. |
| `base_fee_per_gas` | string | Hex wei. `null` for pre-1559 blocks. |

### `txs`

One row per transaction.

| Column | Type | Meaning |
|---|---|---|
| `block_number` | int64 | Join key to `blocks.number`. |
| `tx_hash` | string | Transaction hash, lowercase `0x`-prefixed. |
| `from_address` | string | Sender address, lowercase `0x`-prefixed. |
| `to_address` | string or null | Recipient address, null for contract creation. |
| `value` | string | Hex wei transferred. |
| `gas_used` | int64 | Actual gas consumed. |
| `effective_gas_price` | string | Hex wei actually paid. |
| `gas_price` | string or null | Hex wei legacy gas price. |
| `max_priority_fee_per_gas` | string or null | Hex wei EIP-1559 tip. |
| `type` | int64 | `0` legacy, `1` access list, `2` EIP-1559. |
| `status` | int64 | `1` success, `0` failure. |
| `input_prefix` | string | `"0x"` plus the 4-byte selector, or empty string for value-only transfers. |

### `events`

One row per log entry.

| Column | Type | Meaning |
|---|---|---|
| `block_number` | int64 | Block join key. |
| `tx_hash` | string | Transaction join key. |
| `log_index` | string | Hex-encoded log index within the block. |
| `address` | string | Emitting contract address. |
| `topic0` | string | Event signature hash. |
| `topic1` | string or null | First indexed argument. |
| `topic2` | string or null | Second indexed argument. |
| `topic3` | string or null | Third indexed argument. |
| `data` | string | Hex-encoded non-indexed event data. |

## Lookup Files

The archive also serves static JSON lookups under `/lookups/`.

| Lookup | Key | Purpose |
|---|---|---|
| Function selectors | `lookups/function_selectors.json` | Maps 4-byte selectors to signatures and ABI fragments. |
| Event topics | `lookups/event_topics.json` | Maps `topic0` hashes to signatures and ABI fragments. |

How they relate to parquet fields:

- `txs.input_prefix` is designed to join against function selector lookups
- `events.topic0` is designed to join against event topic lookups

Typical usage:

1. Read `manifest.lookups`.
2. Fetch the JSON file at the published `key`.
3. Verify size and MD5 if you are managing your own cache.
4. Use `txs.input_prefix` or `events.topic0` as the lookup key.

The SDKs expose lookup metadata via the manifest model, but they do not
currently provide first-class helper methods for downloading or decoding those
JSON files.

## Raw Hex Conventions

If you disable SDK decoding or hand the parquet URL to another tool, these raw
value conventions matter:

- wei-scale integers are stored as lowercase `0x`-prefixed hex strings
- `log_index` is also stored as hex upstream
- `null` means missing or not applicable
- `"0x"` means numeric zero
- `""` should be treated as null-like

SDK decode behavior:

- Python converts known wei fields to `DECIMAL(38,0)` and `log_index` to
  `int64`
- TypeScript converts known wei fields to `bigint` and `log_index` to `number`
- TypeScript decode happens in `loadDay()` and `loadRange()`, not in
  `avacache/duckdb`

## Versioning And Compatibility

`schema_version` is the public compatibility boundary.

Examples of changes that require a new schema version:

- add, remove, rename, or retype a parquet column
- change the meaning of an existing column
- change key layout or file naming conventions

Examples of changes that do not require a new schema version:

- adding new dates
- adding optional manifest fields that older clients can ignore
- adding new lookup files
- rebuilding a day's parquet and changing only its `md5` and `size`

Practical guarantees for consumers:

- once a `(date, kind)` appears in `files[]`, it should not disappear
- an existing date may be rebuilt, so cache keys should include `md5`
- `latest_complete_date` should not move backward within a schema version
- column order is stable within a schema version and should match `columns`

## When To Use The SDK Docs Instead

Use this contract guide for:

- manifest semantics
- schema details
- lookup-file usage
- versioning and rebuild rules

Use the language-specific READMEs for:

- installation
- runtime-specific cache behavior
- workflow examples
- DuckDB helper usage
