import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { mkdtemp } from 'node:fs/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Client } from '../src/client.js';
import type { Cache } from '../src/cache.js';

/**
 * Tests serve real parquet bytes (committed under tests/fixtures/) through
 * a fetch stub. hyparquet is the parsing authority, so we exercise it on
 * actual archive bytes rather than a synthetic encoder.
 */

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), 'fixtures');
const DATES = ['2020-09-23', '2020-09-24'] as const;
const DEFAULT_DATE = DATES[1];

async function loadBytes(date: string, kind: string): Promise<Uint8Array> {
  const buf = await readFile(`${FIXTURES}/${date}.${kind}.parquet`);
  return Uint8Array.from(buf);
}

function md5Hex(b: Uint8Array): string {
  return createHash('md5').update(b).digest('hex');
}

async function buildFixture(
  dates: readonly string[] = DATES,
): Promise<{
  manifest: unknown;
  bodies: Record<string, Uint8Array>;
}> {
  const kinds = ['blocks', 'txs', 'events'] as const;
  const bodies: Record<string, Uint8Array> = {};
  const files: unknown[] = [];
  for (const date of dates) {
    for (const kind of kinds) {
      const body = await loadBytes(date, kind);
      bodies[`daily/${date}.${kind}.parquet`] = body;
      files.push({
        date,
        kind,
        key: `daily/${date}.${kind}.parquet`,
        size: body.byteLength,
        md5: md5Hex(body),
        schema_version: 'v2',
      });
    }
  }
  return {
    manifest: {
      chain_id: 43114,
      generated_at: '2026-04-19T00:00:00Z',
      schema_version: 'v2',
      latest_complete_date: dates[dates.length - 1],
      columns: { blocks: [], txs: [], events: [] },
      lookups: {},
      files,
    },
    bodies,
  };
}

function mockFetch(
  baseUrl: string,
  manifest: unknown,
  bodies: Record<string, Uint8Array>,
): typeof fetch {
  return (async (input: RequestInfo | URL): Promise<Response> => {
    const url = typeof input === 'string' ? input : (input as URL).toString();
    const path = url.slice(baseUrl.length).replace(/^\//, '');
    if (path === 'manifest.json') {
      return new Response(JSON.stringify(manifest), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    const body = bodies[path];
    if (!body) return new Response('not found', { status: 404 });
    return new Response(body, { status: 200 });
  }) as unknown as typeof fetch;
}

describe('Client', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'avacache-test-'));
  });

  afterEach(async () => {
    const { rm } = await import('node:fs/promises');
    await rm(tmpDir, { recursive: true, force: true });
  });

  it('fetches the manifest and lists available dates', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    const m = await c.manifest();
    expect(m.chain_id).toBe(43114);
    expect(await c.availableDates('txs')).toEqual([...DATES]);
    expect(await c.latestCompleteDate()).toBe(DEFAULT_DATE);
  });

  it('iterRange yields decoded rows in date order', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    const seen: string[] = [];
    let totalRows = 0;
    for await (const [date, rows] of c.iterRange(DATES[0], DATES[1], 'blocks')) {
      seen.push(date);
      totalRows += rows.length;
    }
    expect(seen).toEqual([...DATES]);
    expect(totalRows).toBeGreaterThan(0);
  });

  it('loadRange concatenates across days', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    const single = await c.loadDay(DATES[0], 'blocks');
    const both = await c.loadRange(DATES[0], DATES[1], 'blocks');
    expect(both.length).toBeGreaterThan(single.length);
  });

  it('cache hit serves second loadDay without refetch', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    let fetchCount = 0;
    const stub = mockFetch(base, manifest, bodies);
    const counted: typeof fetch = (async (...args) => {
      fetchCount++;
      return stub(...(args as Parameters<typeof fetch>));
    }) as typeof fetch;
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: counted,
    });
    await c.loadDay(DEFAULT_DATE, 'blocks');
    const afterFirst = fetchCount;
    await c.loadDay(DEFAULT_DATE, 'blocks');
    // Second call should not have triggered another HTTP fetch (manifest
    // cached in-memory, parquet served from cacheDir).
    expect(fetchCount).toBe(afterFirst);
  });

  it('loadDay reads rows and decodes hex columns', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    const rows = await c.loadDay(DEFAULT_DATE, 'blocks');
    expect(rows.length).toBeGreaterThan(0);
    const bf = rows[0].base_fee_per_gas;
    // genesis era has no base fee -> null or 0n
    expect(bf === null || typeof bf === 'bigint').toBe(true);
  });

  it('throws on missing date', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    await expect(c.loadDay('1999-01-01', 'txs')).rejects.toThrow(/no txs/);
  });

  it('urlFor builds the expected path', () => {
    const c = new Client({
      baseUrl: 'https://cache.test/',
      fetch: (async () => new Response('')) as unknown as typeof fetch,
    });
    expect(c.urlFor('2026-04-18', 'txs')).toBe(
      'https://cache.test/daily/2026-04-18.txs.parquet',
    );
  });

  it('iterRows yields one row at a time with hex decoded', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    let count = 0;
    let firstBaseFee: unknown = undefined;
    for await (const row of c.iterRows(DEFAULT_DATE, 'blocks')) {
      count++;
      if (firstBaseFee === undefined) firstBaseFee = row.base_fee_per_gas;
    }
    // Compare to loadDay on the same fixture for a parity check.
    const day = await c.loadDay(DEFAULT_DATE, 'blocks');
    expect(count).toBe(day.length);
    expect(firstBaseFee === null || typeof firstBaseFee === 'bigint').toBe(true);
  });

  it('iterRows projects columns when requested', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    let projectedRow: Record<string, unknown> | null = null;
    for await (const row of c.iterRows(DEFAULT_DATE, 'blocks', {
      columns: ['number'],
    })) {
      projectedRow = row;
      break;
    }
    expect(projectedRow).not.toBeNull();
    // Hyparquet only materializes the requested column(s). Other fields like
    // base_fee_per_gas should not appear at all on the projected row.
    expect(Object.keys(projectedRow!)).toEqual(['number']);
    expect(typeof projectedRow!.number === 'bigint' || typeof projectedRow!.number === 'number').toBe(true);
  });

  it('iterRowsRange streams across days with column projection', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    let count = 0;
    for await (const row of c.iterRowsRange(DATES[0], DATES[1], 'blocks', {
      columns: ['number'],
      concurrency: 2,
    })) {
      count++;
      // First row should also be column-projected.
      if (count === 1) expect(Object.keys(row)).toEqual(['number']);
    }
    // Should equal sum of per-day loads.
    const day0 = await c.loadDay(DATES[0], 'blocks');
    const day1 = await c.loadDay(DATES[1], 'blocks');
    expect(count).toBe(day0.length + day1.length);
  });

  it('iterRange byte-prefetch fires fetches before consumer awaits', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();

    let fetchCount = 0;
    const stub = mockFetch(base, manifest, bodies);
    const counted: typeof fetch = (async (...args) => {
      fetchCount++;
      return stub(...(args as Parameters<typeof fetch>));
    }) as typeof fetch;

    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: counted,
    });

    const it = c.iterRange(DATES[0], DATES[1], 'blocks', { concurrency: 2 });
    const first = await it.next();
    expect(first.done).toBe(false);
    // After the first yield, both days' parquets plus the manifest should
    // have been requested — this verifies the prefetch is *bytes*, not
    // row-decoding lazily on each yield.
    expect(fetchCount).toBeGreaterThanOrEqual(3); // 1 manifest + 2 days
    // Drain the rest so we don't leak the generator.
    for await (const _ of it) {
      void _;
    }
  });

  it('offline:true serves manifest and parquet from cache, refuses network', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();

    // Warm: online client populates the cache dir.
    const warm = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    await warm.loadDay(DEFAULT_DATE, 'blocks');

    // Offline: no fetch should fire.
    const failingFetch: typeof fetch = (async () => {
      throw new Error('offline test: fetch should not be called');
    }) as typeof fetch;
    const off = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      offline: true,
      fetch: failingFetch,
    });
    const m = await off.manifest();
    expect(m.chain_id).toBe(43114);
    const rows = await off.loadDay(DEFAULT_DATE, 'blocks');
    expect(rows.length).toBeGreaterThan(0);

    // A date that wasn't warmed should fail loudly with the offline error,
    // not succeed silently or attempt a fetch.
    await expect(off.loadDay(DATES[0], 'blocks')).rejects.toThrow(/offline/);
  });

  it('iterRange does not retain consumed prefetch buffers (bounded memory)', async () => {
    // Regression test for a Codex-flagged bug in iterDayBytes: an earlier
    // version indexed `inFlight[i]` instead of `shift()`-ing, which kept
    // every fetched body alive for the lifetime of the generator. That
    // turned the bounded-memory contract into one whose footprint grew
    // linearly with the date range. This test pauses iteration mid-stream
    // and asserts that consumed buffers are GC-reclaimed.
    if (typeof globalThis.gc !== 'function') {
      // --expose-gc is set in vitest.config.ts; this guard is defensive.
      return;
    }

    const baseBody = await loadBytes(DEFAULT_DATE, 'blocks');
    const md5 = md5Hex(baseBody);

    const N = 20;
    const dates = Array.from({ length: N }, (_, i) => {
      const d = String(i + 1).padStart(2, '0');
      return `2020-10-${d}`;
    });
    const manifest = {
      chain_id: 43114,
      generated_at: '2026-04-19T00:00:00Z',
      schema_version: 'v2',
      latest_complete_date: dates[N - 1],
      columns: { blocks: [], txs: [], events: [] },
      lookups: {},
      files: dates.map((date) => ({
        date,
        kind: 'blocks',
        key: `daily/${date}.blocks.parquet`,
        size: baseBody.byteLength,
        md5,
        schema_version: 'v2',
      })),
    };

    // Each fetch returns a fresh Response so the SDK materializes a fresh
    // Uint8Array per call — distinct objects for distinct WeakRefs.
    const base = 'https://cache.test';
    const fetchImpl: typeof fetch = (async (
      input: RequestInfo | URL,
    ): Promise<Response> => {
      const url = typeof input === 'string' ? input : (input as URL).toString();
      if (url.endsWith('/manifest.json')) {
        return new Response(JSON.stringify(manifest), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      // Copy so the Response body is a fresh, distinct buffer per call.
      return new Response(new Uint8Array(baseBody));
    }) as typeof fetch;

    // Capture every verified buffer the SDK hands to the cache.
    const refs: WeakRef<Uint8Array>[] = [];
    const cache: Cache = {
      async get() {
        return null;
      },
      async put(_key, bytes) {
        // Skip the manifest write — only track parquet bodies.
        if (_key.endsWith('.parquet') || _key.includes('daily')) {
          refs.push(new WeakRef(bytes));
        }
      },
    };

    const c = new Client({ baseUrl: base, fetch: fetchImpl, cache });

    // Iterate 10 days manually (not for-await) so the generator stays
    // suspended while we inspect WeakRef liveness.
    const concurrency = 2;
    const it = c.iterRange(dates[0], dates[N - 1], 'blocks', { concurrency });
    for (let i = 0; i < 10; i++) {
      const r = await it.next();
      expect(r.done).toBe(false);
    }

    // Force GC twice with a microtask gap so that any settled-but-unrooted
    // promise values have a chance to be collected.
    globalThis.gc!();
    await new Promise((resolve) => setTimeout(resolve, 0));
    globalThis.gc!();

    const alive = refs.filter((r) => r.deref() !== undefined).length;
    // Bounded contract: at most `concurrency` prefetched buffers in flight,
    // plus the most recently yielded buffer still in scope. Generous slack
    // here keeps the test resilient to GC timing without losing the bug
    // signal — the regression would leave 10+ buffers alive.
    expect(alive).toBeLessThanOrEqual(concurrency + 2);

    // Drain so the generator returns cleanly (and the test doesn't leak it).
    await it.return?.(undefined);
  });

  it('timeout fires when response body stalls after headers', async () => {
    // Regression test for a Codex-flagged bug in safeFetch: an earlier
    // version cleared the abort timer in `finally` as soon as fetch()
    // resolved, so a server that returned headers quickly but then stalled
    // the body could hang the manifest/parquet read indefinitely despite
    // the advertised `timeoutMs`. The fix moves dispose() to the body
    // consumer.
    const base = 'https://stall.test';

    const stallingFetch: typeof fetch = ((
      _input: RequestInfo | URL,
      init?: RequestInit,
    ): Promise<Response> => {
      // Build a body that emits no chunks and never closes. The fetch's
      // AbortSignal must be the one to terminate it.
      const signal = init?.signal as AbortSignal | undefined;
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          if (signal) {
            const onAbort = (): void => {
              controller.error(
                new DOMException('aborted', 'AbortError') as unknown as Error,
              );
            };
            if (signal.aborted) onAbort();
            else signal.addEventListener('abort', onAbort, { once: true });
          }
          // Otherwise, never enqueue or close — the stream stalls forever.
        },
      });
      return Promise.resolve(
        new Response(body, {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      );
    }) as typeof fetch;

    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: stallingFetch,
      timeoutMs: 50,
    });

    const t0 = Date.now();
    await expect(c.manifest()).rejects.toThrow();
    const elapsed = Date.now() - t0;
    // Should reject in roughly timeoutMs (50ms) plus a generous margin —
    // certainly well under 5 seconds. A bug that only timed out on
    // headers would hang here forever and the test would time out.
    expect(elapsed).toBeLessThan(5000);
  });

  it('offline:true with cold cache rejects manifest fetch', async () => {
    const off = new Client({
      baseUrl: 'https://cache.test',
      cacheDir: tmpDir,
      offline: true,
      fetch: (async () => {
        throw new Error('should not fetch');
      }) as typeof fetch,
    });
    await expect(off.manifest()).rejects.toThrow(/offline/);
  });
});
