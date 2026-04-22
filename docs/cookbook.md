# Cookbook

Practical recipes for working with the public Avalanche C-Chain parquet archive
through the `avacache` SDKs.

This document assumes the current package guides and
[archive contract](archive-contract.md) are the source of truth:

- [../README.md](../README.md)
- [../python/README.md](../python/README.md)
- [../typescript/README.md](../typescript/README.md)
- [archive-contract.md](archive-contract.md)

## Ground Rules

- Dates are UTC calendar days in `YYYY-MM-DD`.
- The manifest is authoritative for availability, schema version, file size,
  and MD5.
- `load_day()` / `loadDay()` is the right default for one date.
- `load_range()` / `loadRange()` is for small to moderate in-memory spans.
- Python `iter_range()` is the safe large-range API.
- Range calls are inclusive, skip missing dates inside the window, and fail
  only if no files exist anywhere in the requested range.

## Load The Latest Complete Day

Do not guess by using "today" or constructing a URL directly. Read the
manifest, then ask for the latest complete date.

### Python

```python
import json
from urllib.request import urlopen

from avacache import Client

with Client() as c:
    manifest = c.manifest()
    latest = c.latest_complete_date()
    if latest is None:
        raise RuntimeError("archive has no complete dates yet")

    blocks = c.load_day(latest, "blocks")
    txs = c.load_day(latest, "txs")
    events = c.load_day(latest, "events")

    selector_url = (
        f"{c.base_url.rstrip('/')}/{manifest.lookups['function_selectors'].key}"
    )
    topic_url = f"{c.base_url.rstrip('/')}/{manifest.lookups['event_topics'].key}"

    with urlopen(selector_url) as response:
        selector_lookup = json.loads(response.read())
    with urlopen(topic_url) as response:
        topic_lookup = json.loads(response.read())

    sample_tx = txs.slice(0, 1).to_pylist()[0]
    sample_event = events.slice(0, 1).to_pylist()[0]

    print(latest)
    print(blocks.num_rows, txs.num_rows, events.num_rows)
    print(
        "tx selector:",
        sample_tx["input_prefix"],
        selector_lookup.get(sample_tx["input_prefix"], {}).get("signature"),
    )
    print(
        "event topic0:",
        sample_event["topic0"],
        topic_lookup.get(sample_event["topic0"], {}).get("signature"),
    )
```

### TypeScript

```ts
import { Client } from 'avacache';

const c = new Client();
const manifest = await c.manifest();
const latest = await c.latestCompleteDate();

if (!latest) {
  throw new Error('archive has no complete dates yet');
}

const [blocks, txs, events, selectorLookup, topicLookup] = await Promise.all([
  c.loadDay(latest, 'blocks'),
  c.loadDay(latest, 'txs'),
  c.loadDay(latest, 'events'),
  fetch(
    `${c.baseUrl.replace(/\/$/, '')}/${manifest.lookups.function_selectors.key}`,
  ).then((response) => response.json()),
  fetch(
    `${c.baseUrl.replace(/\/$/, '')}/${manifest.lookups.event_topics.key}`,
  ).then((response) => response.json()),
]);

const sampleTx = txs[0];
const sampleEvent = events[0];

console.log(latest, blocks.length, txs.length, events.length);
console.log(
  'tx selector:',
  sampleTx?.input_prefix,
  selectorLookup[sampleTx?.input_prefix as string]?.signature,
);
console.log(
  'event topic0:',
  sampleEvent?.topic0,
  topicLookup[sampleEvent?.topic0 as string]?.signature,
);
```

Use this pattern when you want the freshest day that is known to be complete
for all three kinds and you also want immediate human-readable labels for
`txs.input_prefix` and `events.topic0`.

For production lookup caches, verify the raw JSON body against the published
`size` and `md5` in `manifest.lookups` as shown in [lookups.md](lookups.md).

## Enumerate Available Dates And Spot Gaps

If you are building a backfill, dashboard, or batch analysis, enumerate dates
from the manifest first instead of probing URLs.

### Python

```python
from avacache import Client

with Client() as c:
    dates = c.available_dates("txs")
    print(f"{len(dates)} tx dates available")
    print("first:", dates[0], "last:", dates[-1])

    gaps = []
    for prev, cur in zip(dates, dates[1:]):
        delta = (cur - prev).days
        if delta > 1:
            gaps.append((prev, cur, delta - 1))

    print("gaps:", gaps[:5])
```

### TypeScript

```ts
import { Client } from 'avacache';

const c = new Client();
const dates = await c.availableDates('txs');

console.log(`${dates.length} tx dates available`);
console.log('first:', dates[0], 'last:', dates[dates.length - 1]);

const gaps: Array<{ after: string; before: string; missingDays: number }> = [];
for (let i = 1; i < dates.length; i += 1) {
  const prev = new Date(`${dates[i - 1]}T00:00:00Z`);
  const cur = new Date(`${dates[i]}T00:00:00Z`);
  const deltaDays = (cur.getTime() - prev.getTime()) / 86_400_000;
  if (deltaDays > 1) {
    gaps.push({
      after: dates[i - 1],
      before: dates[i],
      missingDays: deltaDays - 1,
    });
  }
}

console.log(gaps.slice(0, 5));
```

This is the right base for any "what dates can I trust?" workflow.

## Load A Range Safely

### Small Windows

For a short span that you truly want as one in-memory object, use the range
load directly.

#### Python

```python
from avacache import Client

with Client() as c:
    txs = c.load_range("2026-04-14", "2026-04-20", "txs")
    print(txs.num_rows)
```

#### TypeScript

```ts
import { Client } from 'avacache';

const c = new Client();
const txs = await c.loadRange('2026-04-14', '2026-04-20', 'txs');
console.log(txs.length);
```

Use this only when the total result comfortably fits in memory.

### Large Windows

For longer spans, load day by day. In Python, `iter_range()` is the intended
streaming API. In TypeScript, the safest current pattern is to enumerate dates
and call `loadDay()` yourself.

#### Python

```python
from avacache import Client
import pyarrow.compute as pc

total_failed = 0

with Client() as c:
    for day, txs in c.iter_range(
        "2026-04-01",
        "2026-04-30",
        "txs",
        concurrency=4,
        progress=True,
    ):
        failed = txs.filter(pc.equal(txs["status"], 0))
        total_failed += failed.num_rows
        print(day, failed.num_rows)

print("total failed:", total_failed)
```

#### TypeScript

```ts
import { Client } from 'avacache';

const c = new Client();
const allDates = await c.availableDates('txs');
const wanted = allDates.filter((d) => d >= '2026-04-01' && d <= '2026-04-30');

let totalFailed = 0;
for (const day of wanted) {
  const txs = await c.loadDay(day, 'txs');
  const failed = txs.filter((row) => row.status === 0);
  totalFailed += failed.length;
  console.log(day, failed.length);
}

console.log('total failed:', totalFailed);
```

This avoids building one huge array for a month-long or quarter-long analysis.

## Join Blocks, Transactions, And Events

The archive is intentionally simple:

- Join `blocks.number` to `txs.block_number`.
- Join `txs.tx_hash` to `events.tx_hash`.
- Use `events.block_number` as a consistency check or for block-level grouping.

For row-level transaction analysis, `tx_hash` is the most precise join key.
For block-level rollups, `block_number` is the natural join key.

### Python: One-Day Join In DuckDB

```python
from avacache import open_day

with open_day("2026-04-18") as con:
    rows = con.execute("""
        SELECT
            b.number AS block_number,
            t.tx_hash,
            t.status,
            COUNT(e.tx_hash) AS event_count
        FROM txs t
        JOIN blocks b
          ON b.number = t.block_number
        LEFT JOIN events e
          ON e.tx_hash = t.tx_hash
        GROUP BY 1, 2, 3
        ORDER BY event_count DESC
        LIMIT 20
    """).fetchall()

print(rows[:3])
```

### TypeScript: One-Day Join In Memory

```ts
import { Client } from 'avacache';

const c = new Client();
const [blocks, txs, events] = await Promise.all([
  c.loadDay('2026-04-18', 'blocks'),
  c.loadDay('2026-04-18', 'txs'),
  c.loadDay('2026-04-18', 'events'),
]);

const blockByNumber = new Map(blocks.map((row) => [row.number, row]));
const eventCountByTxHash = new Map<string, number>();

for (const event of events) {
  const txHash = event.tx_hash as string;
  eventCountByTxHash.set(txHash, (eventCountByTxHash.get(txHash) ?? 0) + 1);
}

const joined = txs.slice(0, 5).map((tx) => ({
  tx_hash: tx.tx_hash,
  status: tx.status,
  block_timestamp: blockByNumber.get(tx.block_number)?.timestamp ?? null,
  event_count: eventCountByTxHash.get(tx.tx_hash as string) ?? 0,
}));

console.log(joined);
```

If you are doing serious relational analysis over many days, hand off verified
parquet URLs to a database engine rather than joining giant in-memory objects.

## Query Failed Transactions

`txs.status` is `1` for success and `0` for failure.

### Python: Failed Transactions Over A Week

```python
from avacache import open_range

with open_range("2026-04-14", "2026-04-20") as con:
    rows = con.execute("""
        SELECT
            block_number,
            COUNT(*) AS failed_txs
        FROM txs
        WHERE status = 0
        GROUP BY block_number
        ORDER BY failed_txs DESC
        LIMIT 20
    """).fetchall()

print(rows[:5])
```

### TypeScript: Failed Transactions For One Day

```ts
import { Client } from 'avacache';

const c = new Client();
const txs = await c.loadDay('2026-04-18', 'txs');

const failed = txs.filter((row) => row.status === 0);
console.log('failed:', failed.length);
console.log(failed.slice(0, 3));
```

For multi-day TypeScript analysis, prefer looping over `loadDay()` or handing
verified URLs to DuckDB, Polars, Spark, or another engine.

## Hand Off Verified URLs To Another Engine

If another engine will read parquet for you, do not build URLs blindly. Use the
manifest to confirm availability first, then hand off the URL.

### Python

```python
from avacache import Client

date = "2026-04-18"

with Client() as c:
    available = set(c.available_dates("events"))
    if date not in {d.isoformat() for d in available}:
        raise FileNotFoundError(f"events not available for {date}")

    url = c.url_for(date, "events")
    print(url)
```

### TypeScript

```ts
import { Client } from 'avacache';

const c = new Client();
const date = '2026-04-18';

const available = new Set(await c.availableDates('events'));
if (!available.has(date)) {
  throw new Error(`events not available for ${date}`);
}

const url = c.urlFor(date, 'events');
console.log(url);
```

Use this when:

- DuckDB, Polars, Spark, or another engine will scan the parquet directly
- you want to batch-generate a list of known-good URLs
- you need the raw parquet as published, not SDK-decoded rows

Remember that direct parquet readers will see the raw archive values:

- wei-scale numbers are hex strings upstream
- `events.log_index` is hex upstream
- `"0x"` means numeric zero
- `""` is null-like

## Warm Cache Online, Then Run Offline

### Python: Strict Offline Support

Python supports a real offline mode. Warm the cache while online, then reopen
with `offline=True`.

```python
from avacache import Client

cache_dir = "/tmp/avacache-cookbook"

with Client(cache_dir=cache_dir, offline=False) as online:
    online.load_day("2026-04-18", "blocks")
    online.load_day("2026-04-18", "txs")
    online.load_day("2026-04-18", "events")

with Client(cache_dir=cache_dir, offline=True) as offline:
    txs = offline.load_day("2026-04-18", "txs")
    print(txs.num_rows)
```

This is the right pattern for repeatable notebook work, air-gapped analysis, or
CI tasks that should fail fast on a cache miss.

### TypeScript: Warm Cache For Reuse

TypeScript does not expose a strict offline switch. You can still warm the
cache and benefit from cache hits later, but network use is not forbidden by
the API in the same way Python's offline mode is.

```ts
import { Client } from 'avacache';

const cacheDir = '/tmp/avacache-ts-cookbook';

const warm = new Client({ cacheDir });
await warm.loadDay('2026-04-18', 'blocks');
await warm.loadDay('2026-04-18', 'txs');
await warm.loadDay('2026-04-18', 'events');

const reuse = new Client({ cacheDir });
const txs = await reuse.loadDay('2026-04-18', 'txs');
console.log(txs.length);
```

In Node, the warmed cache lives on disk. In browsers, it lives in IndexedDB.

## Refresh The Manifest When Freshness Matters

Both SDKs memoize the manifest in-process for a short window. If you are
polling for newly completed days, force a refresh.

### Python

```python
from avacache import Client

with Client() as c:
    fresh = c.manifest(force=True)
    print(fresh.generated_at, fresh.latest_complete_date)
```

### TypeScript

```ts
import { Client } from 'avacache';

const c = new Client();
const fresh = await c.manifest(true);
console.log(fresh.generated_at, fresh.latest_complete_date);
```

Use the forced refresh path for polling jobs, not for every request.
