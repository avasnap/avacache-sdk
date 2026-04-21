# Lookup Files

This guide covers the static JSON lookup files published under
`https://parquet.avacache.com/lookups/` and referenced from
`manifest.json`.

Use this when you want to turn:

- `txs.input_prefix` into a function signature such as
  `transfer(address,uint256)`
- `events.topic0` into an event signature such as
  `Transfer(address,address,uint256)`

The archive contract for these files lives in
[docs/archive-contract.md](/ssd/aidev/avacache-sdk/docs/archive-contract.md)
and `../cache/docs/ARCHIVE_SCHEMA.md`.

## What `manifest.lookups` Contains

The manifest publishes lookup metadata by logical name:

```json
{
  "lookups": {
    "function_selectors": {
      "key": "lookups/function_selectors.json",
      "size": 123,
      "md5": "..."
    },
    "event_topics": {
      "key": "lookups/event_topics.json",
      "size": 456,
      "md5": "..."
    }
  }
}
```

Each lookup entry has the same metadata fields as parquet file entries:

| Field | Meaning |
|---|---|
| `key` | Bucket-relative object key. Build the URL as `base_url + "/" + key`. |
| `size` | Expected object size in bytes. |
| `md5` | Expected MD5 of the raw JSON object body. |

Practical rules:

- treat `manifest.json` as authoritative for lookup availability
- do not hardcode object size or MD5
- if you cache lookup files yourself, key by at least `key` and `md5`
- refresh the manifest first if you need the latest lookup metadata

## JSON Shapes

### `lookups/function_selectors.json`

Shape:

```json
{
  "0xa9059cbb": {
    "signature": "transfer(address,uint256)",
    "abi": [
      {
        "type": "function",
        "name": "transfer",
        "inputs": [
          {"name": "to", "type": "address"},
          {"name": "value", "type": "uint256"}
        ]
      }
    ]
  }
}
```

Notes:

- keys are lowercase `0x`-prefixed 4-byte selectors
- these keys are designed to match `txs.input_prefix`
- `txs.input_prefix` may be an empty string for value-only transfers; that will
  not resolve through the lookup

### `lookups/event_topics.json`

Shape:

```json
{
  "0xddf252ad...": {
    "signature": "Transfer(address,address,uint256)",
    "abi": [
      {
        "type": "event",
        "name": "Transfer",
        "anonymous": false,
        "inputs": [
          {"name": "from", "type": "address", "indexed": true},
          {"name": "to", "type": "address", "indexed": true},
          {"name": "value", "type": "uint256", "indexed": false}
        ]
      }
    ]
  }
}
```

Notes:

- keys are lowercase `0x`-prefixed `topic0` hashes
- these keys are designed to match `events.topic0`
- missing keys are normal; not every selector or event hash is guaranteed to be
  present

## Fetch And Verify From Manifest Metadata

Recommended flow:

1. Fetch `manifest.json`.
2. Read `manifest.lookups["function_selectors"]` or
   `manifest.lookups["event_topics"]`.
3. Download the JSON object at the published `key`.
4. Verify the downloaded byte length against `size`.
5. Verify the MD5 of the raw response body against `md5`.
6. Decode the JSON and use parquet values as direct lookup keys.

Why verify the raw body:

- `size` and `md5` are defined on the object body as served
- verifying the parsed JSON string after reformatting is not equivalent
- if a lookup file is rebuilt in place, the manifest tells you exactly when
  your cache is stale

## Python Example

This example uses the Python SDK for manifest metadata, then downloads and
verifies `function_selectors.json` with the standard library.

```python
from __future__ import annotations

import hashlib
import json
from urllib.request import urlopen

from avacache import Client


def fetch_lookup(client: Client, lookup_name: str) -> dict[str, object]:
    manifest = client.manifest()
    entry = manifest.lookups[lookup_name]
    url = f"{client.base_url.rstrip('/')}/{entry.key}"

    with urlopen(url) as response:
        body = response.read()

    if len(body) != entry.size:
        raise ValueError(
            f"{lookup_name} size mismatch: got {len(body)}, expected {entry.size}"
        )

    digest = hashlib.md5(body).hexdigest()
    if digest != entry.md5:
        raise ValueError(
            f"{lookup_name} md5 mismatch: got {digest}, expected {entry.md5}"
        )

    return json.loads(body)


client = Client()
selector_lookup = fetch_lookup(client, "function_selectors")
topic_lookup = fetch_lookup(client, "event_topics")

day = client.latest_complete_date()
if day is None:
    raise RuntimeError("archive has no complete day yet")

txs = client.load_day(day, "txs").to_pylist()
events = client.load_day(day, "events").to_pylist()

for row in txs[:5]:
    selector = row["input_prefix"]
    if not selector:
        print("tx", row["tx_hash"], "has no selector")
        continue
    match = selector_lookup.get(selector)
    signature = match["signature"] if match else None
    print("tx", row["tx_hash"], selector, signature)

for row in events[:5]:
    topic0 = row["topic0"]
    match = topic_lookup.get(topic0)
    signature = match["signature"] if match else None
    print("event", row["tx_hash"], topic0, signature)
```

Behavior notes:

- `client.load_day()` does not download lookup files for you
- `txs.input_prefix` is already a string key; no extra decoding is needed
- `events.topic0` remains a string key; no extra decoding is needed
- `client.manifest(force=True)` forces a fresh manifest fetch if you suspect the
  lookup metadata changed

## TypeScript Example

This example uses the TypeScript SDK for manifest metadata, then downloads and
verifies the lookup JSON in Node.

```ts
import { createHash } from 'node:crypto';
import { Client } from 'avacache';

async function fetchLookup(
  client: Client,
  lookupName: 'function_selectors' | 'event_topics',
) {
  const manifest = await client.manifest();
  const entry = manifest.lookups[lookupName];
  const url = `${client.baseUrl.replace(/\/$/, '')}/${entry.key}`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`failed to fetch ${lookupName}: ${response.status}`);
  }

  const body = Buffer.from(await response.arrayBuffer());
  if (body.length !== entry.size) {
    throw new Error(
      `${lookupName} size mismatch: got ${body.length}, expected ${entry.size}`,
    );
  }

  const digest = createHash('md5').update(body).digest('hex');
  if (digest !== entry.md5) {
    throw new Error(
      `${lookupName} md5 mismatch: got ${digest}, expected ${entry.md5}`,
    );
  }

  return JSON.parse(body.toString('utf8')) as Record<
    string,
    { signature: string; abi: object[] }
  >;
}

const client = new Client();
const selectorLookup = await fetchLookup(client, 'function_selectors');
const topicLookup = await fetchLookup(client, 'event_topics');

const day = await client.latestCompleteDate();
if (!day) throw new Error('archive has no complete day yet');

const txs = await client.loadDay(day, 'txs');
const events = await client.loadDay(day, 'events');

for (const row of txs.slice(0, 5)) {
  const selector = row.input_prefix;
  if (!selector) {
    console.log('tx', row.tx_hash, 'has no selector');
    continue;
  }
  console.log('tx', row.tx_hash, selector, selectorLookup[selector]?.signature);
}

for (const row of events.slice(0, 5)) {
  const topic0 = row.topic0;
  console.log('event', row.tx_hash, topic0, topicLookup[topic0]?.signature);
}
```

Behavior notes:

- `client.loadDay()` resolves parquet rows, not lookup JSON
- TypeScript decode settings do not affect `input_prefix` or `topic0`; both
  stay string keys
- `await client.manifest(true)` forces a fresh manifest fetch

## Failure Modes To Expect

Common cases:

- missing lookup metadata in `manifest.lookups`
  This means the current archive version does not publish that lookup.
- `size` mismatch
  The download is incomplete or you are reading stale/corrupt cached bytes.
- `md5` mismatch
  The body does not match the manifest entry; discard it and refetch after a
  fresh manifest read.
- selector or topic key not found in the JSON map
  Treat as unresolved metadata, not as archive corruption.

## When To Use Lookups Versus ABI Decoders

Use the published lookup files when you need:

- fast human-readable labels for common selectors and event signatures
- a stable reference file that matches the archive contract
- lightweight enrichment keyed directly by `txs.input_prefix` or `events.topic0`

Use a full ABI decoder when you need:

- argument-level decoding of calldata or log `data`
- contract-specific overload resolution beyond the published signature map
- richer type information than `signature` and `abi` fragments alone
