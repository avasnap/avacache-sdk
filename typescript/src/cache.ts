/**
 * Cache adapters for parquet bytes.
 *
 * Three implementations:
 *   - NodeFsCache    — ~/.cache/avacache/v1/<chain>/daily/  (Node)
 *   - IndexedDbCache — browser, IndexedDB-backed
 *   - NoopCache      — cache disabled
 *
 * Selection is automatic in `Client` based on the runtime. Override via
 * `Client({ cache: ... })` for SSR or testing.
 */

export interface Cache {
  get(key: string): Promise<Uint8Array | null>;
  put(key: string, bytes: Uint8Array): Promise<void>;
}

export class NoopCache implements Cache {
  async get(): Promise<null> {
    return null;
  }
  async put(): Promise<void> {
    /* no-op */
  }
}

// ---------------------------------------------------------------------------
// Node

export async function makeNodeFsCache(
  chainId: number,
  overrideDir?: string,
): Promise<Cache> {
  const { promises: fs } = await import('node:fs');
  const path = await import('node:path');
  const os = await import('node:os');
  const crypto = await import('node:crypto');

  const root =
    overrideDir ??
    process.env.AVACACHE_CACHE_DIR ??
    path.join(os.homedir(), '.cache', 'avacache', 'v1');
  const dir = path.join(root, String(chainId));
  await fs.mkdir(dir, { recursive: true });
  const dirResolved = path.resolve(dir);

  // Resolve `key` under `dir` and reject anything that walks outside it.
  // The key is also regex-validated by the manifest schema, but defense in
  // depth: this code is the last line before fs.writeFile.
  const resolveSafe = (key: string): string => {
    if (
      !key ||
      key.includes('\0') ||
      key.includes('\\') ||
      key.startsWith('/') ||
      key.split('/').includes('..')
    ) {
      throw new Error(`unsafe cache key: ${JSON.stringify(key)}`);
    }
    const escaped = key.replace(/\//g, '__');
    const final = path.resolve(dir, escaped);
    if (final !== dirResolved && !final.startsWith(dirResolved + path.sep)) {
      throw new Error(`cache key escapes root: ${JSON.stringify(key)}`);
    }
    return final;
  };

  return {
    async get(key: string): Promise<Uint8Array | null> {
      try {
        const buf = await fs.readFile(resolveSafe(key));
        // Copy so the returned view doesn't alias Node's internal Buffer pool.
        return Uint8Array.from(buf);
      } catch {
        return null;
      }
    },
    async put(key: string, bytes: Uint8Array): Promise<void> {
      const final = resolveSafe(key);
      // PID + random suffix makes tmp names unguessable; O_EXCL ('wx')
      // prevents racing writers from clobbering each other; O_NOFOLLOW
      // neutralizes a pre-placed symlink in a shared cache dir.
      const tmp = `${final}.${process.pid}-${crypto.randomBytes(4).toString('hex')}.tmp`;
      const fsSync = await import('node:fs');
      const flags = fsSync.constants.O_WRONLY |
        fsSync.constants.O_CREAT |
        fsSync.constants.O_EXCL |
        (fsSync.constants.O_NOFOLLOW ?? 0);
      let handle: import('node:fs/promises').FileHandle | undefined;
      try {
        handle = await fs.open(tmp, flags, 0o600);
        await handle.writeFile(bytes);
        await handle.close();
        handle = undefined;
        await fs.rename(tmp, final);
      } catch (err) {
        if (handle) await handle.close().catch(() => {});
        await fs.unlink(tmp).catch(() => {});
        throw err;
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Browser

export function makeIndexedDbCache(chainId: number): Cache {
  const dbName = `avacache-v1-${chainId}`;
  const store = 'blobs';

  const open = () =>
    new Promise<IDBDatabase>((resolve, reject) => {
      const req = indexedDB.open(dbName, 1);
      req.onupgradeneeded = () => req.result.createObjectStore(store);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });

  return {
    async get(key: string): Promise<Uint8Array | null> {
      const db = await open();
      return new Promise((resolve, reject) => {
        const tx = db.transaction(store, 'readonly');
        const req = tx.objectStore(store).get(key);
        req.onsuccess = () => {
          const v = req.result as ArrayBuffer | Uint8Array | undefined;
          if (!v) return resolve(null);
          // Accept both legacy ArrayBuffer entries and new Uint8Array entries.
          resolve(v instanceof Uint8Array ? new Uint8Array(v) : new Uint8Array(v));
        };
        req.onerror = () => reject(req.error);
      });
    },
    async put(key: string, bytes: Uint8Array): Promise<void> {
      const db = await open();
      return new Promise((resolve, reject) => {
        const tx = db.transaction(store, 'readwrite');
        // Store the Uint8Array directly. Storing `bytes.buffer` would persist
        // the underlying ArrayBuffer, which can be larger than the view (e.g.
        // when bytes is a sub-view of a Node Buffer pool), causing size
        // mismatches on retrieval.
        tx.objectStore(store).put(bytes, key);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
      });
    },
  };
}

// ---------------------------------------------------------------------------

export function isBrowser(): boolean {
  return typeof window !== 'undefined' && typeof indexedDB !== 'undefined';
}

export function isNode(): boolean {
  return (
    typeof process !== 'undefined' &&
    typeof (process as { versions?: { node?: string } }).versions?.node === 'string'
  );
}
