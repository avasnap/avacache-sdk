import {
  ManifestSchema,
  manifestDates,
  manifestEntry,
  type FileEntry,
  type Kind,
  type Manifest,
} from './manifest.js';
import { decodeRow, type Row } from './hex.js';
import { readRowsFromBytes } from './parquet.js';
import {
  NoopCache,
  isBrowser,
  isNode,
  makeIndexedDbCache,
  makeNodeFsCache,
  type Cache,
} from './cache.js';

export const DEFAULT_BASE_URL = 'https://parquet.avacache.com';
export const DEFAULT_CHAIN_ID = 43114;
const MANIFEST_TTL_MS = 5 * 60 * 1000;
const MANIFEST_MAX_BYTES = 16 * 1024 * 1024; // 16 MiB cap on manifest body
const DEFAULT_REQUEST_TIMEOUT_MS = 60_000;

export interface ClientOptions {
  chainId?: number;
  baseUrl?: string;
  cache?: Cache | false;
  cacheDir?: string;
  /** Decode hex numeric columns to bigint/number (default: true). */
  decodeHex?: boolean;
  fetch?: typeof fetch;
  /** Per-request timeout in ms. Default 60000. */
  timeoutMs?: number;
}

export interface LoadOptions {
  /** Override the per-client `decodeHex` setting. */
  decodeHex?: boolean;
}

function md5Hex(bytes: Uint8Array): Promise<string> {
  if (isNode()) {
    return import('node:crypto').then(({ createHash }) =>
      createHash('md5').update(bytes).digest('hex'),
    );
  }
  // Browsers: WebCrypto has no MD5. Skip md5 verification in browser — we
  // still verify size.
  return Promise.resolve('');
}

function validateBaseUrl(url: string): string {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error(`AVACACHE base URL is not a valid URL: ${url}`);
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new Error(
      `AVACACHE base URL must be http(s), got ${parsed.protocol} in ${url}`,
    );
  }
  return url;
}

/** Read a Response body to a Uint8Array, aborting if `maxBytes` is exceeded. */
async function readBodyCapped(
  resp: Response,
  maxBytes: number,
  context: string,
): Promise<Uint8Array> {
  const body = resp.body;
  if (!body) {
    // No streaming reader available — fall back to arrayBuffer with a
    // post-hoc check. Better than nothing.
    const buf = new Uint8Array(await resp.arrayBuffer());
    if (buf.byteLength > maxBytes) {
      throw new Error(
        `${context}: body of ${buf.byteLength} bytes exceeds cap of ${maxBytes}`,
      );
    }
    return buf;
  }
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel().catch(() => {});
      throw new Error(
        `${context}: response exceeded cap of ${maxBytes} bytes`,
      );
    }
    chunks.push(value);
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

export class Client {
  readonly chainId: number;
  readonly baseUrl: string;
  readonly decodeHex: boolean;
  readonly timeoutMs: number;
  private readonly fetchOverride: typeof fetch | undefined;
  private readonly cachePromise: Promise<Cache>;

  private _manifest: Manifest | null = null;
  private _manifestAt = 0;
  // md5s already verified on disk this process — avoids re-hashing on every
  // loadDay call within a long-lived Client.
  private readonly _verified = new Set<string>();

  constructor(opts: ClientOptions = {}) {
    this.chainId = opts.chainId ?? DEFAULT_CHAIN_ID;
    const envBase =
      typeof process !== 'undefined' && process.env
        ? process.env.AVACACHE_BASE_URL
        : undefined;
    const rawBase = (opts.baseUrl ?? envBase ?? DEFAULT_BASE_URL).replace(/\/+$/, '');
    this.baseUrl = validateBaseUrl(rawBase);
    this.decodeHex = opts.decodeHex ?? true;
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    // Don't bind globalThis.fetch eagerly — that crashes Node <18 even when
    // the caller is about to inject opts.fetch. Resolve lazily in fetch().
    this.fetchOverride = opts.fetch;

    if (opts.cache === false) {
      this.cachePromise = Promise.resolve(new NoopCache());
    } else if (opts.cache) {
      this.cachePromise = Promise.resolve(opts.cache);
    } else if (isNode()) {
      this.cachePromise = makeNodeFsCache(this.chainId, opts.cacheDir);
    } else if (isBrowser()) {
      this.cachePromise = Promise.resolve(makeIndexedDbCache(this.chainId));
    } else {
      this.cachePromise = Promise.resolve(new NoopCache());
    }
  }

  private async fetch(url: string, context: string): Promise<Response> {
    const fetchFn = this.fetchOverride ?? globalThis.fetch;
    if (!fetchFn) {
      throw new Error(
        'no fetch available — pass opts.fetch or run on Node 18+ / a browser',
      );
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const resp = await fetchFn(url, {
        // 'error' rejects the promise on any 3xx — the archive is a flat
        // bucket, so a redirect means the origin is misconfigured or
        // compromised.
        redirect: 'error',
        signal: controller.signal,
      });
      if (!resp.ok) {
        throw new Error(`${context}: ${resp.status} ${resp.statusText}`);
      }
      return resp;
    } finally {
      clearTimeout(timer);
    }
  }

  // -------------------------------------------------------------- URLs

  urlFor(date: string, kind: Kind): string {
    return `${this.baseUrl}/daily/${date}.${kind}.parquet`;
  }

  manifestUrl(): string {
    return `${this.baseUrl}/manifest.json`;
  }

  // -------------------------------------------------------------- Manifest

  async manifest(force = false): Promise<Manifest> {
    const now = Date.now();
    if (!force && this._manifest && now - this._manifestAt < MANIFEST_TTL_MS) {
      return this._manifest;
    }
    const resp = await this.fetch(this.manifestUrl(), 'manifest fetch');
    const bytes = await readBodyCapped(
      resp,
      MANIFEST_MAX_BYTES,
      'manifest fetch',
    );
    const raw = JSON.parse(new TextDecoder().decode(bytes));
    const manifest = ManifestSchema.parse(raw);
    if (manifest.chain_id !== this.chainId) {
      throw new Error(
        `manifest chain_id ${manifest.chain_id} does not match ` +
          `configured chain_id ${this.chainId}`,
      );
    }
    this._manifest = manifest;
    this._manifestAt = now;
    return this._manifest;
  }

  async availableDates(kind: Kind): Promise<string[]> {
    return manifestDates(await this.manifest(), kind);
  }

  async latestCompleteDate(): Promise<string | null> {
    const m = await this.manifest();
    return m.latest_complete_date ?? null;
  }

  // -------------------------------------------------------------- Load

  async loadDay(
    date: string,
    kind: Kind,
    opts: LoadOptions = {},
  ): Promise<Row[]> {
    const m = await this.manifest();
    const entry = manifestEntry(m, date, kind);
    if (!entry) {
      throw new Error(
        `no ${kind} parquet for ${date} in manifest ` +
          `(latest=${m.latest_complete_date ?? 'n/a'})`,
      );
    }
    const bytes = await this.fetchBytes(entry);
    const rows = await readRowsFromBytes(bytes);

    const decode = opts.decodeHex ?? this.decodeHex;
    if (decode) for (const r of rows) decodeRow(r, kind);
    return rows;
  }

  async loadRange(
    start: string,
    end: string,
    kind: Kind,
    opts: LoadOptions = {},
  ): Promise<Row[]> {
    if (end < start) throw new Error('end before start');
    const m = await this.manifest();
    const dates = manifestDates(m, kind).filter((d) => d >= start && d <= end);
    if (dates.length === 0) {
      throw new Error(`no ${kind} files in range ${start}..${end}`);
    }
    const all: Row[] = [];
    for (const d of dates) {
      const rows = await this.loadDay(d, kind, opts);
      for (const r of rows) all.push(r);
    }
    return all;
  }

  // -------------------------------------------------------------- Internals

  private async fetchBytes(entry: FileEntry): Promise<Uint8Array> {
    const cache = await this.cachePromise;
    const cacheKey = `${entry.key}|${entry.md5}`;

    const cached = await cache.get(cacheKey);
    if (cached && cached.byteLength === entry.size) {
      // Cache hit: re-verify md5 once per process (Node only — browsers have
      // no md5 in WebCrypto, documented intentional behavior).
      if (this._verified.has(entry.md5)) return cached;
      const cachedMd5 = await md5Hex(cached);
      if (!cachedMd5 || cachedMd5 === entry.md5) {
        // Either Node verified, or browser path (md5 unavailable, size-only).
        this._verified.add(entry.md5);
        return cached;
      }
      // Node and md5 mismatched — fall through to re-download.
    }

    const resp = await this.fetch(
      `${this.baseUrl}/${entry.key}`,
      `parquet fetch for ${entry.key}`,
    );
    // Cap the body at exactly entry.size + 1 byte: if the origin streams more
    // than entry.size, abort — a hostile or broken origin can't OOM us.
    const bytes = await readBodyCapped(
      resp,
      entry.size + 1,
      `parquet fetch for ${entry.key}`,
    );

    if (bytes.byteLength !== entry.size) {
      throw new Error(
        `size mismatch for ${entry.key}: got ${bytes.byteLength}, want ${entry.size}`,
      );
    }
    const got = await md5Hex(bytes);
    if (got && got !== entry.md5) {
      throw new Error(
        `md5 mismatch for ${entry.key}: got ${got}, want ${entry.md5}`,
      );
    }

    await cache.put(cacheKey, bytes);
    this._verified.add(entry.md5);
    return bytes;
  }
}
