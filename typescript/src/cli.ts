/**
 * avacache CLI — thin wrapper over the SDK for shell / agent use.
 *
 * Stable contract is documented in .claude/skills/avacache/SKILL.md, not
 * here. Subcommands and flags can change between minor versions; consult
 * the skill.
 *
 * Node-only. The browser bundle does not include this entry.
 */

import { createHash, randomBytes } from 'node:crypto';
import { open as fsOpen, rename, unlink } from 'node:fs/promises';
import { Client, type Kind } from './index.js';

type Format = 'table' | 'ndjson' | 'json' | 'csv';

interface Args {
  cmd: string;
  positional: string[];
  flags: Map<string, string | true>;
}

// Flags that take no value. Without this set, `--no-decode-hex show 2026-04-18 txs`
// would consume `show` as the flag's value and the date would become the
// command. Boolean flags must be declared explicitly.
const BOOLEAN_FLAGS = new Set([
  'no-decode-hex',
  'json',
  'force',
  'help',
]);

function parseArgs(argv: string[]): Args {
  const positional: string[] = [];
  const flags = new Map<string, string | true>();
  let i = 0;
  let cmd = '';
  while (i < argv.length) {
    const a = argv[i]!;
    if (a.startsWith('--')) {
      const eq = a.indexOf('=');
      const name = eq !== -1 ? a.slice(2, eq) : a.slice(2);
      if (eq !== -1) {
        flags.set(name, a.slice(eq + 1));
      } else if (BOOLEAN_FLAGS.has(name)) {
        flags.set(name, true);
      } else {
        const next = argv[i + 1];
        if (next !== undefined && !next.startsWith('--')) {
          flags.set(name, next);
          i += 1;
        } else {
          flags.set(name, true);
        }
      }
    } else if (!cmd) {
      cmd = a;
    } else {
      positional.push(a);
    }
    i += 1;
  }
  return { cmd, positional, flags };
}

function flag(args: Args, name: string): string | undefined {
  const v = args.flags.get(name);
  return typeof v === 'string' ? v : undefined;
}

function bool(args: Args, name: string): boolean {
  return args.flags.has(name);
}

function requireKind(s: string | undefined): Kind {
  if (s === 'blocks' || s === 'txs' || s === 'events') return s;
  throw new Error(`expected kind in {blocks,txs,events}, got ${JSON.stringify(s)}`);
}

function jsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Uint8Array) return Buffer.from(value).toString('hex');
  return value;
}

function defaultFormat(): Format {
  return process.stdout.isTTY ? 'table' : 'ndjson';
}

function formatTable(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return '(no rows)';
  const cols = Object.keys(rows[0]!);
  const widths = cols.map((c) =>
    Math.max(c.length, ...rows.map((r) => String(r[c] ?? '').length)),
  );
  const fmtRow = (vals: string[]) =>
    vals.map((v, i) => v.padEnd(widths[i]!)).join('  ');
  const out: string[] = [];
  out.push(fmtRow(cols));
  out.push(fmtRow(widths.map((w) => '-'.repeat(w))));
  for (const r of rows) {
    out.push(fmtRow(cols.map((c) => String(r[c] ?? ''))));
  }
  return out.join('\n');
}

function emitRows(rows: Record<string, unknown>[], fmt: Format, limit: number | null): void {
  const sliced = limit === null ? rows : rows.slice(0, limit);
  switch (fmt) {
    case 'table':
      process.stdout.write(formatTable(sliced) + '\n');
      return;
    case 'ndjson':
      for (const r of sliced) process.stdout.write(JSON.stringify(r, jsonReplacer) + '\n');
      return;
    case 'json':
      process.stdout.write(JSON.stringify(sliced, jsonReplacer) + '\n');
      return;
    case 'csv': {
      if (sliced.length === 0) return;
      const cols = Object.keys(sliced[0]!);
      const esc = (v: unknown): string => {
        if (v === null || v === undefined) return '';
        const s = typeof v === 'bigint' ? v.toString() : String(v);
        return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
      };
      process.stdout.write(cols.join(',') + '\n');
      for (const r of sliced) process.stdout.write(cols.map((c) => esc(r[c])).join(',') + '\n');
      return;
    }
  }
}

function clientFromArgs(args: Args): Client {
  return new Client({
    baseUrl: flag(args, 'base-url'),
    cacheDir: flag(args, 'cache-dir'),
    chainId: flag(args, 'chain-id') ? Number(flag(args, 'chain-id')) : undefined,
    decodeHex: bool(args, 'no-decode-hex') ? false : undefined,
  });
}

function help(): void {
  process.stdout.write(
    `Usage: avacache [global flags] <command> [args]

Thin CLI over the avacache SDK. See .claude/skills/avacache/SKILL.md for the
agent contract.

Global flags:
  --base-url <url>     Override archive base URL (or AVACACHE_BASE_URL).
  --cache-dir <path>   Override cache dir (or AVACACHE_CACHE_DIR).
  --chain-id <n>       Override chain id (default 43114).
  --no-decode-hex      Skip hex decoding; emit raw upstream strings.

Commands:
  manifest [--json] [--force]            Show manifest summary or full JSON.
  latest                                 Print latest_complete_date.
  dates [--kind blocks|txs|events]       Print available dates, one per line.
  url <date> <kind>                      Print verified parquet URL.
  fetch <date> <kind> --out <path>       Download verified parquet to a file.
  show <date> <kind> [--limit N]         Load and print rows.
        [--format table|ndjson|json|csv]
  lookup selector|topic <0x..> [--json]  Resolve via published lookup files.
`,
  );
}

/**
 * Stream the lookup file, hash + size-verify on the fly, and return only
 * the value for `key`. The published `function_selectors.json` is ~650MB
 * — larger than V8's max string length — so we can't JSON.parse the whole
 * body. The shape is uniform `{"0x..":[...]}` so we scan for `"<key>":`
 * and bracket-walk the value.
 */
async function fetchLookupKey(
  client: Client,
  name: 'function_selectors' | 'event_topics',
  key: string,
): Promise<unknown[] | null> {
  const m = await client.manifest();
  const entry = m.lookups[name];
  if (!entry) throw new Error(`manifest does not publish lookup ${name}`);
  // Route through the client's safeguarded fetch so the CLI inherits the
  // same redirect rejection, request timeout, and non-2xx error handling
  // that the rest of the SDK applies to manifest/parquet downloads.
  const { response, dispose } = await client.safeFetch(
    `${client.baseUrl}/${entry.key}`,
    `lookup fetch for ${name}`,
  );
  if (!response.body) {
    dispose();
    throw new Error(`lookup fetch for ${name} returned no body`);
  }

  const target = `"${key.toLowerCase()}":`;
  const reader = response.body.getReader();
  const hasher = createHash('md5');
  const decoder = new TextDecoder('utf-8');

  // Sliding text window. Capacity = max(target length, plausible
  // 1-chunk-spanning value length). 1MB is far larger than any single
  // value entry we expect.
  const overlapBytes = 1024 * 1024;
  let textWindow = '';
  let totalBytes = 0;
  let foundIdx = -1;
  let captured = '';
  let depth = 0;
  let inString = false;
  let escaping = false;
  let capturing = false;

  const consumeForCapture = (chunk: string): boolean => {
    for (let i = 0; i < chunk.length; i += 1) {
      const ch = chunk[i]!;
      captured += ch;
      if (escaping) { escaping = false; continue; }
      if (ch === '\\' && inString) { escaping = true; continue; }
      if (ch === '"') { inString = !inString; continue; }
      if (inString) continue;
      if (ch === '[' || ch === '{') depth += 1;
      else if (ch === ']' || ch === '}') {
        depth -= 1;
        if (depth === 0) return true;
      }
    }
    return false;
  };

  // Hold the timeout across the entire body read; the timer covers stalls
  // in the streaming body, not just time-to-headers. dispose() always runs.
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      hasher.update(value);
      const chunkText = decoder.decode(value, { stream: true });

      if (capturing) {
        if (consumeForCapture(chunkText)) break;
        continue;
      }

      textWindow += chunkText;
      foundIdx = textWindow.indexOf(target);
      if (foundIdx !== -1) {
        const after = textWindow.slice(foundIdx + target.length);
        capturing = true;
        if (consumeForCapture(after)) break;
      } else if (textWindow.length > overlapBytes) {
        // Keep only the trailing overlap so a key spanning two chunks still resolves.
        textWindow = textWindow.slice(textWindow.length - overlapBytes);
      }
    }

    // Drain remaining bytes so size + md5 are checked even after a match.
    while (capturing) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      hasher.update(value);
    }
  } finally {
    dispose();
  }

  if (totalBytes !== entry.size) {
    throw new Error(`${name} size mismatch: got ${totalBytes}, expected ${entry.size}`);
  }
  const digest = hasher.digest('hex');
  if (digest !== entry.md5) {
    throw new Error(`${name} md5 mismatch: got ${digest}, expected ${entry.md5}`);
  }

  if (!captured) return null;
  // `captured` is the value text starting at `[` (or `{` for legacy single-object entries) ending at the matching closer.
  const parsed = JSON.parse(captured);
  return Array.isArray(parsed) ? parsed : [parsed];
}

async function cmdManifest(client: Client, args: Args): Promise<number> {
  const m = await client.manifest(bool(args, 'force'));
  if (bool(args, 'json')) {
    process.stdout.write(JSON.stringify(m, null, 2) + '\n');
    return 0;
  }
  const byKind: Record<string, { date: string }[]> = { blocks: [], txs: [], events: [] };
  let totalBytes = 0;
  for (const f of m.files) {
    (byKind[f.kind] ??= []).push(f);
    totalBytes += f.size;
  }
  process.stdout.write(`chain_id:             ${m.chain_id}\n`);
  process.stdout.write(`schema_version:       ${m.schema_version}\n`);
  process.stdout.write(`generated_at:         ${m.generated_at}\n`);
  process.stdout.write(`latest_complete_date: ${m.latest_complete_date ?? '(none)'}\n`);
  process.stdout.write(`files:                ${m.files.length} (${(totalBytes / 1024 ** 3).toFixed(2)} GiB)\n`);
  for (const kind of Object.keys(byKind)) {
    const entries = byKind[kind]!;
    if (entries.length === 0) continue;
    const dates = entries.map((e) => e.date).sort();
    process.stdout.write(`  ${kind.padEnd(7)} ${entries.length} files  ${dates[0]}..${dates[dates.length - 1]}\n`);
  }
  const lookups = Object.keys(m.lookups).sort();
  if (lookups.length) process.stdout.write(`lookups:              ${lookups.join(', ')}\n`);
  return 0;
}

async function cmdLatest(client: Client): Promise<number> {
  const latest = await client.latestCompleteDate();
  if (!latest) return 1;
  process.stdout.write(latest + '\n');
  return 0;
}

async function cmdDates(client: Client, args: Args): Promise<number> {
  const kind = requireKind(flag(args, 'kind') ?? 'txs');
  const dates = await client.availableDates(kind);
  for (const d of dates) process.stdout.write(d + '\n');
  return 0;
}

async function cmdUrl(client: Client, args: Args): Promise<number> {
  const [date, kindStr] = args.positional;
  if (!date || !kindStr) throw new Error('usage: avacache url <date> <kind>');
  const kind = requireKind(kindStr);
  const m = await client.manifest();
  const entry = m.files.find((f) => f.date === date && f.kind === kind);
  if (!entry) {
    process.stderr.write(`no ${kind} parquet for ${date} in manifest\n`);
    return 2;
  }
  process.stdout.write(`${client.baseUrl}/${entry.key}\n`);
  return 0;
}

async function cmdFetch(client: Client, args: Args): Promise<number> {
  const [date, kindStr] = args.positional;
  if (!date || !kindStr) throw new Error('usage: avacache fetch <date> <kind> --out <path>');
  const out = flag(args, 'out');
  if (!out) throw new Error('--out <path> is required for fetch');
  const kind = requireKind(kindStr);
  const m = await client.manifest();
  const entry = m.files.find((f) => f.date === date && f.kind === kind);
  if (!entry) {
    process.stderr.write(`no ${kind} parquet for ${date} in manifest\n`);
    return 2;
  }
  // Stream parquet to disk while hashing on the fly. Daily `txs` and
  // `events` files can be hundreds of MB to multiple GB, so buffering via
  // `arrayBuffer()` would OOM. Verify size+md5 before atomic-renaming
  // into place; on any failure, the temp file is cleaned up.
  //
  // Route through `client.safeFetch` so the CLI inherits redirect
  // rejection, request timeout, and non-2xx error handling — the same
  // safeguards `loadDay` applies to its own parquet downloads.
  const { response, dispose } = await client.safeFetch(
    `${client.baseUrl}/${entry.key}`,
    `parquet fetch for ${entry.key}`,
  );
  // Single outer try guarantees `dispose()` runs even if the local file
  // handle can't be opened (e.g. --out points to an unwritable directory).
  // Without this, an early fsOpen failure would leak the watchdog timer
  // until it fired.
  try {
    if (!response.body) {
      throw new Error(`parquet fetch for ${entry.key} returned no body`);
    }
    const tmp = `${out}.${process.pid}-${randomBytes(4).toString('hex')}.tmp`;
    const handle = await fsOpen(tmp, 'wx', 0o600);
    const hasher = createHash('md5');
    let written = 0;
    try {
      const reader = response.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        written += value.byteLength;
        if (written > entry.size) {
          throw new Error(
            `oversized download for ${entry.key}: received >${entry.size} bytes`,
          );
        }
        hasher.update(value);
        await handle.write(value);
      }
      await handle.close();
      if (written !== entry.size) {
        throw new Error(`size mismatch: got ${written}, expected ${entry.size}`);
      }
      const digest = hasher.digest('hex');
      if (digest !== entry.md5) {
        throw new Error(`md5 mismatch: got ${digest}, expected ${entry.md5}`);
      }
      await rename(tmp, out);
    } catch (err) {
      await handle.close().catch(() => {});
      await unlink(tmp).catch(() => {});
      throw err;
    }
  } finally {
    dispose();
  }
  process.stdout.write(out + '\n');
  return 0;
}

async function cmdShow(client: Client, args: Args): Promise<number> {
  const [date, kindStr] = args.positional;
  if (!date || !kindStr) throw new Error('usage: avacache show <date> <kind>');
  const kind = requireKind(kindStr);
  const limitStr = flag(args, 'limit');
  const limit = limitStr === undefined ? 20 : Number(limitStr);
  const fmt = (flag(args, 'format') as Format | undefined) ?? defaultFormat();
  const rows = await client.loadDay(date, kind);
  emitRows(rows, fmt, limit === 0 ? null : limit);
  return 0;
}

async function cmdLookup(client: Client, args: Args): Promise<number> {
  const [kindStr, key] = args.positional;
  if (!kindStr || !key) throw new Error('usage: avacache lookup selector|topic <0x...>');
  if (kindStr !== 'selector' && kindStr !== 'topic') {
    throw new Error(`expected selector|topic, got ${kindStr}`);
  }
  const name = kindStr === 'selector' ? 'function_selectors' : 'event_topics';
  const list = await fetchLookupKey(client, name, key);
  if (list === null) {
    process.stderr.write(`no entry for ${key} in ${name}\n`);
    return 1;
  }
  // Live archive returns a list of candidates (selector/topic collisions).
  // Sort by `count` desc so the most common signature is first.
  const sorted = [...list].sort((a, b) =>
    Number((b as { count?: number }).count ?? 0) - Number((a as { count?: number }).count ?? 0),
  );
  if (bool(args, 'json')) {
    process.stdout.write(JSON.stringify(sorted, null, 2) + '\n');
  } else {
    for (const c of sorted) {
      const cc = c as { signature?: string; count?: number };
      const sig = cc.signature ?? '';
      const count = cc.count;
      process.stdout.write(count !== undefined ? `${sig}\t${count}\n` : `${sig}\n`);
    }
  }
  return 0;
}

const DISPATCH: Record<string, (c: Client, a: Args) => Promise<number>> = {
  manifest: cmdManifest,
  latest: (c) => cmdLatest(c),
  dates: cmdDates,
  url: cmdUrl,
  fetch: cmdFetch,
  show: cmdShow,
  lookup: cmdLookup,
};

export async function main(argv: string[] = process.argv.slice(2)): Promise<number> {
  if (argv.length === 0 || argv[0] === '--help' || argv[0] === '-h') {
    help();
    return 0;
  }
  const args = parseArgs(argv);
  const handler = DISPATCH[args.cmd];
  if (!handler) {
    process.stderr.write(`unknown command: ${args.cmd}\n`);
    help();
    return 64;
  }
  const client = clientFromArgs(args);
  try {
    return await handler(client, args);
  } catch (err) {
    process.stderr.write(`error: ${(err as Error).message}\n`);
    return 1;
  }
}

