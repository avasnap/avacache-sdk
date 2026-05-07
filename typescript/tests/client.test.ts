import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { mkdtemp } from 'node:fs/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Client } from '../src/client.js';

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
});
