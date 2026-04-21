import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { mkdtemp } from 'node:fs/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Client } from '../src/client.js';

/**
 * We don't ship synthetic parquet here — hyparquet is the authority on
 * parsing. Instead, we mock the HTTP layer with a fetch stub that serves
 * the repo's actual 2020-09-24.*.parquet files (tiny — a few hundred
 * bytes each) and a matching manifest.
 */

const REPO_CACHE = '/ssd/aidev/cache/43114';
const DATE = '2020-09-24';

async function loadBytes(kind: string): Promise<Uint8Array> {
  const buf = await readFile(`${REPO_CACHE}/${DATE}.${kind}.parquet`);
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}

function md5Hex(b: Uint8Array): string {
  return createHash('md5').update(b).digest('hex');
}

async function buildFixture(): Promise<{
  manifest: unknown;
  bodies: Record<string, Uint8Array>;
}> {
  const kinds = ['blocks', 'txs', 'events'] as const;
  const bodies: Record<string, Uint8Array> = {};
  const files: unknown[] = [];
  for (const kind of kinds) {
    const body = await loadBytes(kind);
    bodies[`daily/${DATE}.${kind}.parquet`] = body;
    files.push({
      date: DATE,
      kind,
      key: `daily/${DATE}.${kind}.parquet`,
      size: body.byteLength,
      md5: md5Hex(body),
      schema_version: 'v2',
    });
  }
  return {
    manifest: {
      chain_id: 43114,
      generated_at: '2026-04-19T00:00:00Z',
      schema_version: 'v2',
      latest_complete_date: DATE,
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
    expect(await c.availableDates('txs')).toEqual([DATE]);
    expect(await c.latestCompleteDate()).toBe(DATE);
  });

  it('loadDay reads rows and decodes hex columns', async () => {
    const base = 'https://cache.test';
    const { manifest, bodies } = await buildFixture();
    const c = new Client({
      baseUrl: base,
      cacheDir: tmpDir,
      fetch: mockFetch(base, manifest, bodies),
    });
    const rows = await c.loadDay(DATE, 'blocks');
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
