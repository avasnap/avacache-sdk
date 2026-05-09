import {
  ManifestSchema,
  manifestDates,
  manifestEntry,
  type FileEntry,
  type Kind,
  type Manifest,
} from './manifest.js';
import { decodeRow, type Row } from './hex.js';
import { iterRowsFromBytes, readRowsFromBytes } from './parquet.js';
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
  /** EVM chain id of the archive to read. Default 43114 (Avalanche C-Chain). */
  chainId?: number;
  /**
   * Archive base URL. Trailing slashes are stripped. Must be `http(s)://`.
   * Falls back to `process.env.AVACACHE_BASE_URL`, then to the public bucket.
   */
  baseUrl?: string;
  /**
   * Custom cache adapter, or `false` to disable caching entirely.
   * If omitted: Node uses an FS cache, browsers use IndexedDB, other
   * runtimes (workers/edge) fall back to a no-op cache.
   */
  cache?: Cache | false;
  /**
   * Cache directory for the Node FS cache. Falls back to
   * `process.env.AVACACHE_CACHE_DIR`, then to `~/.cache/avacache/v1/`.
   * Ignored when running in a browser.
   */
  cacheDir?: string;
  /** Decode hex numeric columns to bigint/number on load. Default true. */
  decodeHex?: boolean;
  /**
   * Override `globalThis.fetch`. Useful for SSR and tests. Resolved lazily
   * so passing a custom fetch works on Node versions without a global one.
   */
  fetch?: typeof fetch;
  /** Per-request timeout in ms. Default 60000. */
  timeoutMs?: number;
  /**
   * Refuse network access; serve only from the local cache. Falls back to
   * `process.env.AVACACHE_OFFLINE` (`'1'` / `'true'` / `'yes'`).
   *
   * In offline mode the client throws on any cache miss — manifest fetches
   * included. Warm the cache with an online client first if you need a fresh
   * manifest before going offline.
   */
  offline?: boolean;
}

export interface LoadOptions {
  /** Override the per-client `decodeHex` setting for this call. */
  decodeHex?: boolean;
}

export interface IterRangeOptions extends LoadOptions {
  /**
   * Max files downloading in parallel while the consumer iterates.
   * Default 4. Memory cost: roughly `concurrency` raw parquet bodies in
   * flight, plus one decoded day yielded to the consumer.
   */
  concurrency?: number;
}

export interface IterRowsOptions extends LoadOptions {
  /**
   * Project only these column names. Hyparquet skips the column-chunk decode
   * for everything else, which is the single biggest memory and CPU lever
   * for wide tables (notably `events`). Hex decode is still applied to the
   * projected columns; columns absent from the projection are simply not in
   * the yielded row.
   */
  columns?: readonly string[];
}

export interface IterRowsRangeOptions extends IterRowsOptions {
  /**
   * Max parquet files downloading in parallel while the consumer iterates.
   * Default 4. Memory cost: roughly `concurrency` raw parquet bodies in
   * flight, plus one row group's worth of decoded rows for the day being
   * yielded.
   */
  concurrency?: number;
}

/**
 * Cache key under which we persist the manifest JSON. Must not collide with
 * the parquet/lookup key shapes (`daily/...`, `lookups/...`) that the
 * manifest itself can publish.
 */
const MANIFEST_CACHE_KEY = '__manifest__.json';

function offlineDefault(): boolean {
  if (typeof process === 'undefined' || !process.env) return false;
  const v = (process.env.AVACACHE_OFFLINE ?? '').toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
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

/**
 * Client for the public Avalanche C-Chain parquet archive.
 *
 * Reads `manifest.json` from the archive root, then fetches per-day
 * `blocks` / `txs` / `events` parquet files, verifying size and md5 against
 * the manifest before handing decoded rows to the caller.
 *
 * @example
 *   const c = new Client();
 *   const rows = await c.loadDay('2026-04-18', 'txs');
 *   for await (const [date, day] of c.iterRange('2026-01-01', '2026-04-18', 'events')) {
 *     // process one day at a time, bounded memory
 *   }
 */
export class Client {
  readonly chainId: number;
  readonly baseUrl: string;
  readonly decodeHex: boolean;
  readonly timeoutMs: number;
  readonly offline: boolean;
  private readonly fetchOverride: typeof fetch | undefined;
  private readonly cachePromise: Promise<Cache>;

  private _manifest: Manifest | null = null;
  private _manifestAt = 0;
  // md5s already verified on disk this process — avoids re-hashing on every
  // loadDay call within a long-lived Client.
  private readonly _verified = new Set<string>();

  /** Construct a client. All options have sensible defaults; see `ClientOptions`. */
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
    this.offline = opts.offline ?? offlineDefault();
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

  /**
   * Issue an archive request that inherits the client's safety contract:
   * the configured fetch (override or `globalThis.fetch`), redirect
   * rejection (the archive is a flat bucket — any 3xx means the origin is
   * misconfigured or compromised), a thrown error on non-2xx, and an
   * idle-stall timeout that fires if the response body doesn't make
   * progress for `timeoutMs`. Use this for any direct archive download
   * outside the built-in `loadDay` / `loadRange` paths (e.g. lookup JSON,
   * ad-hoc fetches by the bundled CLI) so those callers get the same
   * guarantees as the rest of the client.
   *
   * The timeout is an **idle deadline**, not a total-download deadline —
   * each body chunk that arrives refreshes it. A multi-GB parquet on a
   * slow link is fine; an origin that returns headers and then stalls is
   * aborted after `timeoutMs` of silence. The caller MUST invoke
   * `dispose()` after consuming (or abandoning) the response body so the
   * timer is cleared; forgetting to dispose leaks a setTimeout handle
   * until the next idle window expires, and calling dispose before the
   * body is fully read leaves stalled body streams unprotected.
   */
  async safeFetch(
    url: string,
    context: string,
  ): Promise<{ response: Response; dispose: () => void }> {
    if (this.offline) {
      throw new Error(
        `${context}: client is offline (AVACACHE_OFFLINE=1 or offline:true) — refusing to fetch ${url}`,
      );
    }
    const fetchFn = this.fetchOverride ?? globalThis.fetch;
    if (!fetchFn) {
      throw new Error(
        'no fetch available — pass opts.fetch or run on Node 18+ / a browser',
      );
    }
    const controller = new AbortController();
    const timeoutMs = this.timeoutMs;
    // Idle-stall watchdog. The initial countdown covers time-to-headers;
    // once body chunks start arriving, the TransformStream below resets
    // the timer on every chunk, so a body that progresses (however
    // slowly) won't be aborted, but one that stalls for `timeoutMs` will.
    let timer = setTimeout(() => controller.abort(), timeoutMs);
    const refresh = (): void => {
      clearTimeout(timer);
      timer = setTimeout(() => controller.abort(), timeoutMs);
    };
    const dispose = (): void => clearTimeout(timer);

    let raw: Response;
    try {
      raw = await fetchFn(url, {
        redirect: 'error',
        signal: controller.signal,
      });
      if (!raw.ok) {
        dispose();
        throw new Error(`${context}: ${raw.status} ${raw.statusText}`);
      }
    } catch (err) {
      dispose();
      throw err;
    }

    if (!raw.body) {
      // No body to watchdog (e.g. some 204s). Caller still must dispose
      // to clear the time-to-headers timer if they hold the response.
      return { response: raw, dispose };
    }
    // Wrap the body so each chunk that flows through refreshes the timer.
    // The fetch's AbortSignal is bound to body reads, so when the timer
    // fires the reader on the wrapped stream throws — the consumer sees
    // a normal abort/error rather than a silent hang.
    const transformed = raw.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, ctrl) {
          refresh();
          ctrl.enqueue(chunk);
        },
      }),
    );
    const response = new Response(transformed, {
      status: raw.status,
      statusText: raw.statusText,
      headers: raw.headers,
    });
    return { response, dispose };
  }

  // -------------------------------------------------------------- URLs

  /** Build the public archive URL for a given day's parquet of `kind`. */
  urlFor(date: string, kind: Kind): string {
    return `${this.baseUrl}/daily/${date}.${kind}.parquet`;
  }

  /** URL of the manifest.json at the archive root. */
  manifestUrl(): string {
    return `${this.baseUrl}/manifest.json`;
  }

  // -------------------------------------------------------------- Manifest

  /**
   * Fetch and parse `manifest.json`. The result is cached in memory for 5
   * minutes; pass `force` to re-fetch immediately. The manifest's
   * `chain_id` is cross-checked against the configured chain — a mismatch
   * throws rather than poisoning the cache namespace.
   */
  async manifest(force = false): Promise<Manifest> {
    const now = Date.now();
    if (!force && this._manifest && now - this._manifestAt < MANIFEST_TTL_MS) {
      return this._manifest;
    }
    const cache = await this.cachePromise;

    if (this.offline) {
      // Offline: never call the network. Use the in-memory copy if any,
      // otherwise fall back to the disk-cached manifest from a previous
      // online run; otherwise fail loudly.
      if (this._manifest) return this._manifest;
      const cached = await cache.get(MANIFEST_CACHE_KEY);
      if (!cached) {
        throw new Error(
          'manifest fetch: client is offline and no cached manifest is available — ' +
            'run once online to warm the cache before going offline',
        );
      }
      const manifest = this.parseAndValidateManifest(cached);
      this._manifest = manifest;
      this._manifestAt = now;
      return manifest;
    }

    const { response, dispose } = await this.safeFetch(
      this.manifestUrl(),
      'manifest fetch',
    );
    let bytes: Uint8Array;
    try {
      bytes = await readBodyCapped(response, MANIFEST_MAX_BYTES, 'manifest fetch');
    } finally {
      dispose();
    }
    const manifest = this.parseAndValidateManifest(bytes);

    // Persist the raw body so an offline run on the same cache dir can
    // recover the exact JSON. We await the write — fire-and-forget would
    // race with a caller who immediately constructs an offline client on
    // the same dir, and with test cleanup that rm's the cache dir. Failures
    // are swallowed so a NoopCache or read-only cache adapter is fine —
    // the in-memory copy is still good.
    try {
      await cache.put(MANIFEST_CACHE_KEY, bytes);
    } catch {
      /* non-fatal */
    }

    this._manifest = manifest;
    this._manifestAt = now;
    return manifest;
  }

  private parseAndValidateManifest(bytes: Uint8Array): Manifest {
    const raw = JSON.parse(new TextDecoder().decode(bytes));
    const manifest = ManifestSchema.parse(raw);
    if (manifest.chain_id !== this.chainId) {
      throw new Error(
        `manifest chain_id ${manifest.chain_id} does not match ` +
          `configured chain_id ${this.chainId}`,
      );
    }
    return manifest;
  }

  /**
   * List the dates (YYYY-MM-DD) for which the manifest has a parquet of
   * `kind`. Returns an empty array if the kind has no files yet — typos
   * like `'block'` are caught at compile time by the `Kind` type.
   */
  async availableDates(kind: Kind): Promise<string[]> {
    return manifestDates(await this.manifest(), kind);
  }

  /**
   * The latest date for which all of `blocks`, `txs`, and `events` are
   * published, per the manifest. Useful as the default upper bound for
   * range queries. Returns null if the manifest doesn't declare one.
   */
  async latestCompleteDate(): Promise<string | null> {
    const m = await this.manifest();
    return m.latest_complete_date ?? null;
  }

  // -------------------------------------------------------------- Load

  /**
   * Resolve a `(date, kind)` to a manifest entry, throwing if missing.
   * Internal helper used by every load/iter path.
   */
  private async resolveEntry(date: string, kind: Kind): Promise<FileEntry> {
    const m = await this.manifest();
    const entry = manifestEntry(m, date, kind);
    if (!entry) {
      throw new Error(
        `no ${kind} parquet for ${date} in manifest ` +
          `(latest=${m.latest_complete_date ?? 'n/a'})`,
      );
    }
    return entry;
  }

  /**
   * Download (or read from cache) one day's parquet of `kind` and return
   * the decoded rows. Throws if the manifest has no entry for that date.
   *
   * Hex columns are decoded by default — set `decodeHex: false` to keep
   * the raw `0x...` strings (e.g. for round-tripping or custom decoding).
   */
  async loadDay(
    date: string,
    kind: Kind,
    opts: LoadOptions = {},
  ): Promise<Row[]> {
    const entry = await this.resolveEntry(date, kind);
    const bytes = await this.fetchBytes(entry);
    const rows = await readRowsFromBytes(bytes);

    const decode = opts.decodeHex ?? this.decodeHex;
    if (decode) for (const r of rows) decodeRow(r, kind);
    return rows;
  }

  /**
   * Load all days in `[start, end]` (inclusive) of `kind` and concatenate
   * into a single array. Bounded prefetch via `concurrency` (default 4).
   *
   * Beware memory: every row is held at once. For multi-month spans of
   * `txs` or `events`, prefer {@link iterRange} (day-at-a-time) or
   * {@link iterRowsRange} (row-at-a-time, with optional column projection).
   */
  async loadRange(
    start: string,
    end: string,
    kind: Kind,
    opts: IterRangeOptions = {},
  ): Promise<Row[]> {
    const all: Row[] = [];
    for await (const [, rows] of this.iterRange(start, end, kind, opts)) {
      for (const r of rows) all.push(r);
    }
    return all;
  }

  /**
   * Yield `[date, rows]` pairs in date order with bounded prefetch. While
   * the consumer processes one day, up to `concurrency` (default 4) more
   * downloads run in the background, so iteration overlaps with I/O.
   *
   * Memory: at any moment we hold roughly `concurrency` raw parquet bodies
   * (verified bytes) plus one decoded day yielded to the consumer. Decode
   * is lazy — the prefetch queue holds bytes only, so a slow consumer does
   * not balloon into N decoded days. Prefer this over {@link loadRange}
   * for multi-month spans; for spans where even one day's `Row[]` won't
   * fit, drop to {@link iterRowsRange}.
   *
   * Throws if the date range contains no files for `kind`, or if `end`
   * precedes `start`.
   */
  async *iterRange(
    start: string,
    end: string,
    kind: Kind,
    opts: IterRangeOptions = {},
  ): AsyncGenerator<[string, Row[]], void, void> {
    const decode = opts.decodeHex ?? this.decodeHex;
    for await (const [date, bytes] of this.iterDayBytes(start, end, kind, opts)) {
      const rows = await readRowsFromBytes(bytes);
      if (decode) for (const r of rows) decodeRow(r, kind);
      yield [date, rows];
    }
  }

  /**
   * Yield decoded rows from a single day one at a time, walking the parquet
   * file row group by row group. Use this when even one day's `Row[]` is
   * too large to hold in heap (typical for full days of `events`).
   *
   * Pass `columns` to project only the fields you need — hyparquet skips
   * the column-chunk decode for everything else, which is the single
   * biggest memory and CPU lever for wide tables.
   */
  async *iterRows(
    date: string,
    kind: Kind,
    opts: IterRowsOptions = {},
  ): AsyncGenerator<Row, void, void> {
    const entry = await this.resolveEntry(date, kind);
    const bytes = await this.fetchBytes(entry);
    const decode = opts.decodeHex ?? this.decodeHex;
    for await (const row of iterRowsFromBytes(bytes, { columns: opts.columns })) {
      if (decode) decodeRow(row, kind);
      yield row;
    }
  }

  /**
   * Yield decoded rows across `[start, end]` (inclusive) one at a time,
   * with bounded byte prefetch. Combines the streaming guarantees of
   * {@link iterRows} with the pipelined I/O of {@link iterRange}.
   *
   * Memory ceiling: roughly `concurrency` raw parquet bodies in flight,
   * plus one row group's worth of decoded rows for the day currently being
   * yielded.
   *
   * Throws if the date range contains no files for `kind`, or if `end`
   * precedes `start`.
   */
  async *iterRowsRange(
    start: string,
    end: string,
    kind: Kind,
    opts: IterRowsRangeOptions = {},
  ): AsyncGenerator<Row, void, void> {
    const decode = opts.decodeHex ?? this.decodeHex;
    for await (const [, bytes] of this.iterDayBytes(start, end, kind, opts)) {
      for await (const row of iterRowsFromBytes(bytes, {
        columns: opts.columns,
      })) {
        if (decode) decodeRow(row, kind);
        yield row;
      }
    }
  }

  /**
   * Internal: yield `[date, rawBytes]` pairs in date order with bounded
   * byte prefetch. Both {@link iterRange} and {@link iterRowsRange} build
   * on this — keeping prefetch as bytes (not decoded rows) means a slow
   * consumer caps memory at `concurrency × parquet_bytes`, regardless of
   * how the consumer chooses to decode.
   */
  private async *iterDayBytes(
    start: string,
    end: string,
    kind: Kind,
    opts: { concurrency?: number },
  ): AsyncGenerator<[string, Uint8Array], void, void> {
    if (end < start) throw new Error('end before start');
    const m = await this.manifest();
    const matched: { date: string; entry: FileEntry }[] = [];
    for (const f of m.files) {
      if (f.kind === kind && f.date >= start && f.date <= end) {
        matched.push({ date: f.date, entry: f });
      }
    }
    matched.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
    if (matched.length === 0) {
      throw new Error(`no ${kind} files in range ${start}..${end}`);
    }

    const concurrency = Math.max(1, opts.concurrency ?? 4);
    // Sliding window of pending byte fetches. We keep at most `concurrency`
    // promises in flight; the queue holds verified bytes, not decoded rows,
    // so peak memory is bounded by parquet body size, not row materialization.
    //
    // Use `shift()` (not array indexing) so consumed promises drop out of
    // the queue and their resolved `Uint8Array` becomes GC-eligible. An
    // earlier version indexed `inFlight[i]`, which kept every previously
    // fetched body alive for the lifetime of the generator — turning a
    // bounded-memory iterator into one whose footprint grew linearly with
    // the date range. The same applies to the local `bytes` binding below:
    // each loop iteration's `const bytes` goes out of scope on the next
    // iteration, so the SDK never holds more than one yielded body at a time.
    const inFlight: Promise<Uint8Array>[] = [];
    const enqueue = (idx: number): void => {
      const p = this.fetchBytes(matched[idx].entry);
      // Attach a no-op handler so V8 doesn't fire an unhandled-rejection
      // warning between scheduling and the eventual await below; the real
      // rejection still surfaces when we `await` the promise.
      p.catch(() => {});
      inFlight.push(p);
    };

    const prime = Math.min(concurrency, matched.length);
    for (let i = 0; i < prime; i++) enqueue(i);

    for (let i = 0; i < matched.length; i++) {
      // Pop the head before scheduling the next prefetch, so the in-flight
      // queue stays at <= concurrency entries even momentarily.
      const head = inFlight.shift()!;
      const nextIdx = i + concurrency;
      if (nextIdx < matched.length) enqueue(nextIdx);
      const bytes = await head;
      yield [matched[i].date, bytes];
    }
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

    const { response, dispose } = await this.safeFetch(
      `${this.baseUrl}/${entry.key}`,
      `parquet fetch for ${entry.key}`,
    );
    let bytes: Uint8Array;
    try {
      // Cap the body at exactly entry.size + 1 byte: if the origin streams
      // more than entry.size, abort — a hostile or broken origin can't OOM
      // us.
      bytes = await readBodyCapped(
        response,
        entry.size + 1,
        `parquet fetch for ${entry.key}`,
      );
    } finally {
      dispose();
    }

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
