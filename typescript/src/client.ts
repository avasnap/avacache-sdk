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

export interface ClientOptions {
  chainId?: number;
  baseUrl?: string;
  cache?: Cache | false;
  cacheDir?: string;
  /** Decode hex numeric columns to bigint/number (default: true). */
  decodeHex?: boolean;
  fetch?: typeof fetch;
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

export class Client {
  readonly chainId: number;
  readonly baseUrl: string;
  readonly decodeHex: boolean;
  private readonly fetchFn: typeof fetch;
  private readonly cachePromise: Promise<Cache>;

  private _manifest: Manifest | null = null;
  private _manifestAt = 0;

  constructor(opts: ClientOptions = {}) {
    this.chainId = opts.chainId ?? DEFAULT_CHAIN_ID;
    this.baseUrl = (opts.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, '');
    this.decodeHex = opts.decodeHex ?? true;
    this.fetchFn = opts.fetch ?? globalThis.fetch.bind(globalThis);

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
    const resp = await this.fetchFn(this.manifestUrl());
    if (!resp.ok) {
      throw new Error(`manifest fetch ${resp.status} ${resp.statusText}`);
    }
    const raw = await resp.json();
    this._manifest = ManifestSchema.parse(raw);
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
    if (cached && cached.byteLength === entry.size) return cached;

    const resp = await this.fetchFn(`${this.baseUrl}/${entry.key}`);
    if (!resp.ok) {
      throw new Error(
        `parquet fetch ${resp.status} ${resp.statusText} for ${entry.key}`,
      );
    }
    const bytes = new Uint8Array(await resp.arrayBuffer());

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
    return bytes;
  }
}
