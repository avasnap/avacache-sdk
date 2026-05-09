import { readFile, mkdtemp, rm, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { main } from '../src/cli.js';

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), 'fixtures');
const DATE = '2020-09-24';
const BASE = 'https://cli.test';

const SAMPLE_SELECTORS = {
  '0xa9059cbb': [
    { signature: 'transfer(address,uint256)', count: 491287, abi: { name: 'transfer' } },
    { signature: 'transfer(bytes4,bytes32)', count: 12, abi: { name: 'transfer' } },
  ],
  '0x70a08231': [
    { signature: 'balanceOf(address)', count: 88000, abi: { name: 'balanceOf' } },
  ],
};

const SAMPLE_TOPICS = {
  '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef': [
    { signature: 'Transfer(address,address,uint256)', count: 603616, abi: {} },
  ],
};

function md5Hex(b: Uint8Array): string {
  return createHash('md5').update(b).digest('hex');
}

async function buildArchive(): Promise<{
  manifest: unknown;
  bodies: Record<string, Uint8Array>;
}> {
  const kinds = ['blocks', 'txs', 'events'] as const;
  const bodies: Record<string, Uint8Array> = {};
  const files: unknown[] = [];
  for (const kind of kinds) {
    const buf = await readFile(`${FIXTURES}/${DATE}.${kind}.parquet`);
    const body = Uint8Array.from(buf);
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
  const selectorsBody = new TextEncoder().encode(JSON.stringify(SAMPLE_SELECTORS));
  const topicsBody = new TextEncoder().encode(JSON.stringify(SAMPLE_TOPICS));
  bodies['lookups/function_selectors.json'] = selectorsBody;
  bodies['lookups/event_topics.json'] = topicsBody;
  return {
    manifest: {
      chain_id: 43114,
      generated_at: '2026-04-19T00:00:00Z',
      schema_version: 'v2',
      latest_complete_date: DATE,
      columns: { blocks: [], txs: [], events: [] },
      lookups: {
        function_selectors: {
          key: 'lookups/function_selectors.json',
          size: selectorsBody.byteLength,
          md5: md5Hex(selectorsBody),
        },
        event_topics: {
          key: 'lookups/event_topics.json',
          size: topicsBody.byteLength,
          md5: md5Hex(topicsBody),
        },
      },
      files,
    },
    bodies,
  };
}

interface FetchOpts {
  /** When set, lookup-file responses stream this many bytes per chunk to exercise the bracket-walk parser. */
  chunkSize?: number;
  /** Override the function_selectors body (e.g. to corrupt md5). */
  selectorsBody?: Uint8Array;
}

function chunkedStream(bytes: Uint8Array, chunkSize: number): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i >= bytes.byteLength) {
        controller.close();
        return;
      }
      const next = bytes.slice(i, Math.min(i + chunkSize, bytes.byteLength));
      i += next.byteLength;
      controller.enqueue(next);
    },
  });
}

function installFetch(
  manifest: unknown,
  bodies: Record<string, Uint8Array>,
  opts: FetchOpts = {},
): () => void {
  const original = globalThis.fetch;
  const stub = (async (input: RequestInfo | URL): Promise<Response> => {
    const url = typeof input === 'string' ? input : (input as URL).toString();
    const path = url.slice(BASE.length).replace(/^\//, '');
    if (path === 'manifest.json') {
      return new Response(JSON.stringify(manifest), { status: 200 });
    }
    let body = bodies[path];
    if (path === 'lookups/function_selectors.json' && opts.selectorsBody) {
      body = opts.selectorsBody;
    }
    if (!body) return new Response('not found', { status: 404 });
    if (path.startsWith('lookups/') && opts.chunkSize) {
      return new Response(chunkedStream(body, opts.chunkSize), { status: 200 });
    }
    return new Response(body, { status: 200 });
  }) as unknown as typeof fetch;
  globalThis.fetch = stub;
  return () => {
    globalThis.fetch = original;
  };
}

interface CapturedOutput {
  stdout: string;
  stderr: string;
}

async function runCli(argv: string[]): Promise<{ code: number; out: CapturedOutput }> {
  const out: CapturedOutput = { stdout: '', stderr: '' };
  const stdoutSpy = vi.spyOn(process.stdout, 'write').mockImplementation((chunk: string | Uint8Array) => {
    out.stdout += typeof chunk === 'string' ? chunk : new TextDecoder().decode(chunk);
    return true;
  });
  const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation((chunk: string | Uint8Array) => {
    out.stderr += typeof chunk === 'string' ? chunk : new TextDecoder().decode(chunk);
    return true;
  });
  try {
    const code = await main(argv);
    return { code, out };
  } finally {
    stdoutSpy.mockRestore();
    stderrSpy.mockRestore();
  }
}

describe('avacache CLI', () => {
  let tmpDir: string;
  let restoreFetch: () => void;
  let manifest: unknown;
  let bodies: Record<string, Uint8Array>;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'avacache-cli-'));
    const fixture = await buildArchive();
    manifest = fixture.manifest;
    bodies = fixture.bodies;
    restoreFetch = installFetch(manifest, bodies);
  });

  afterEach(async () => {
    restoreFetch();
    await rm(tmpDir, { recursive: true, force: true });
  });

  const baseArgs = () => ['--base-url', BASE, '--cache-dir', tmpDir];

  it('manifest summary prints chain_id and latest_complete_date', async () => {
    const { code, out } = await runCli([...baseArgs(), 'manifest']);
    expect(code).toBe(0);
    expect(out.stdout).toContain('chain_id:             43114');
    expect(out.stdout).toContain(`latest_complete_date: ${DATE}`);
    expect(out.stdout).toContain('function_selectors');
  });

  it('manifest --json emits parseable JSON', async () => {
    const { code, out } = await runCli([...baseArgs(), 'manifest', '--json']);
    expect(code).toBe(0);
    const parsed = JSON.parse(out.stdout);
    expect(parsed.chain_id).toBe(43114);
    expect(parsed.latest_complete_date).toBe(DATE);
  });

  it('latest prints the latest complete date', async () => {
    const { code, out } = await runCli([...baseArgs(), 'latest']);
    expect(code).toBe(0);
    expect(out.stdout.trim()).toBe(DATE);
  });

  it('dates lists available dates for a kind', async () => {
    const { code, out } = await runCli([...baseArgs(), 'dates', '--kind', 'txs']);
    expect(code).toBe(0);
    expect(out.stdout.trim()).toBe(DATE);
  });

  it('url prints the verified parquet URL', async () => {
    const { code, out } = await runCli([...baseArgs(), 'url', DATE, 'txs']);
    expect(code).toBe(0);
    expect(out.stdout.trim()).toBe(`${BASE}/daily/${DATE}.txs.parquet`);
  });

  it('url returns exit 2 for unknown date', async () => {
    const { code, out } = await runCli([...baseArgs(), 'url', '1999-01-01', 'txs']);
    expect(code).toBe(2);
    expect(out.stderr).toContain('no txs parquet for 1999-01-01');
  });

  it('fetch writes verified parquet bytes to --out', async () => {
    const target = join(tmpDir, 'out.parquet');
    const { code, out } = await runCli([
      ...baseArgs(), 'fetch', DATE, 'blocks', '--out', target,
    ]);
    expect(code).toBe(0);
    expect(out.stdout.trim()).toBe(target);
    const s = await stat(target);
    expect(s.size).toBe(bodies[`daily/${DATE}.blocks.parquet`]!.byteLength);
  });

  it('--no-decode-hex is a boolean flag and does not swallow the subcommand', async () => {
    // Regression: prior arg parser consumed `show` as the value for
    // `--no-decode-hex`, breaking the documented invocation form. With
    // the boolean-flag fix, `--no-decode-hex` is treated as valueless
    // and the subcommand parses correctly. We assert that values come
    // back as raw `0x...` strings (decoding off) rather than bigint.
    const { code, out } = await runCli([
      ...baseArgs(), '--no-decode-hex',
      'show', DATE, 'txs', '--format', 'ndjson', '--limit', '1',
    ]);
    expect(code).toBe(0);
    const row = JSON.parse(out.stdout.trim().split('\n')[0]!);
    expect(typeof row.value).toBe('string');
    expect(row.value).toMatch(/^0x/);  // raw hex, not a decoded bigint string
  });

  it('show ndjson emits one row per line with bigint values stringified', async () => {
    const { code, out } = await runCli([
      ...baseArgs(), 'show', DATE, 'txs', '--format', 'ndjson', '--limit', '1',
    ]);
    expect(code).toBe(0);
    const line = out.stdout.trim().split('\n')[0]!;
    const row = JSON.parse(line);
    // value is a wei-scale field; decoding produces a bigint, jsonReplacer stringifies it.
    expect(typeof row.value).toBe('string');
  });

  it('lookup selector sorts candidates by count desc', async () => {
    const { code, out } = await runCli([...baseArgs(), 'lookup', 'selector', '0xa9059cbb']);
    expect(code).toBe(0);
    const lines = out.stdout.trim().split('\n');
    expect(lines[0]).toBe('transfer(address,uint256)\t491287');
    expect(lines[1]).toBe('transfer(bytes4,bytes32)\t12');
  });

  it('lookup selector accepts uppercase keys', async () => {
    const { code, out } = await runCli([...baseArgs(), 'lookup', 'selector', '0xA9059CBB']);
    expect(code).toBe(0);
    expect(out.stdout).toContain('transfer(address,uint256)');
  });

  it('lookup --json prints the full candidate list', async () => {
    const { code, out } = await runCli([
      ...baseArgs(), 'lookup', 'topic',
      '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
      '--json',
    ]);
    expect(code).toBe(0);
    const parsed = JSON.parse(out.stdout);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed[0].signature).toBe('Transfer(address,address,uint256)');
    expect(parsed[0].count).toBe(603616);
  });

  it('lookup returns exit 1 for unknown key', async () => {
    const { code, out } = await runCli([...baseArgs(), 'lookup', 'selector', '0xdeadbeef']);
    expect(code).toBe(1);
    expect(out.stderr).toContain('no entry for 0xdeadbeef');
  });

  it('lookup streaming finds key when split across many small chunks', async () => {
    // Re-install fetch with tiny chunks so the bracket-walk parser is
    // forced to handle key-spanning-chunks and value-spanning-chunks.
    restoreFetch();
    restoreFetch = installFetch(manifest, bodies, { chunkSize: 17 });
    const { code, out } = await runCli([...baseArgs(), 'lookup', 'selector', '0x70a08231']);
    expect(code).toBe(0);
    expect(out.stdout.trim()).toBe('balanceOf(address)\t88000');
  });

  it('lookup rejects redirected lookup-file responses', async () => {
    // Regression: cmdLookup must inherit the SDK's redirect: 'error'
    // contract via client.safeFetch. A misconfigured/compromised origin
    // returning a 3xx for the lookup file used to be silently followed
    // by the bare `fetch()` call.
    restoreFetch();
    const original = globalThis.fetch;
    const stub = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = typeof input === 'string' ? input : (input as URL).toString();
      if (url.endsWith('manifest.json')) {
        return new Response(JSON.stringify(manifest), { status: 200 });
      }
      if (url.endsWith('lookups/function_selectors.json')) {
        // Mirror what the platform Fetch does when redirect: 'error'
        // is set: reject the promise rather than expose the 3xx.
        if (init?.redirect === 'error') {
          throw new TypeError('unexpected redirect (test)');
        }
        return new Response(null, { status: 302, headers: { Location: 'https://evil.example/' } });
      }
      return new Response('not found', { status: 404 });
    }) as unknown as typeof fetch;
    globalThis.fetch = stub;
    restoreFetch = () => { globalThis.fetch = original; };

    const { code, out } = await runCli([...baseArgs(), 'lookup', 'selector', '0xa9059cbb']);
    expect(code).toBe(1);
    expect(out.stderr).toMatch(/unexpected redirect/);
  });

  it('fetch rejects redirected parquet responses', async () => {
    restoreFetch();
    const original = globalThis.fetch;
    const stub = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = typeof input === 'string' ? input : (input as URL).toString();
      if (url.endsWith('manifest.json')) {
        return new Response(JSON.stringify(manifest), { status: 200 });
      }
      if (url.endsWith(`daily/${DATE}.blocks.parquet`)) {
        if (init?.redirect === 'error') {
          throw new TypeError('unexpected redirect (test)');
        }
        return new Response(null, { status: 301, headers: { Location: 'https://evil.example/' } });
      }
      return new Response('not found', { status: 404 });
    }) as unknown as typeof fetch;
    globalThis.fetch = stub;
    restoreFetch = () => { globalThis.fetch = original; };

    const target = join(tmpDir, 'redirect.parquet');
    const { code, out } = await runCli([
      ...baseArgs(), 'fetch', DATE, 'blocks', '--out', target,
    ]);
    expect(code).toBe(1);
    expect(out.stderr).toMatch(/unexpected redirect/);
  });

  it('lookup fails on md5 mismatch', async () => {
    restoreFetch();
    // Serve a body that doesn't match the manifest's md5.
    const corrupted = new TextEncoder().encode(JSON.stringify({ '0xa9059cbb': [{ signature: 'wrong', count: 1 }] }));
    restoreFetch = installFetch(manifest, bodies, { selectorsBody: corrupted });
    const { code, out } = await runCli([...baseArgs(), 'lookup', 'selector', '0xa9059cbb']);
    expect(code).toBe(1);
    expect(out.stderr).toMatch(/(md5 mismatch|size mismatch)/);
  });
});
