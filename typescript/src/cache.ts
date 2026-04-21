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

  const root =
    overrideDir ??
    process.env.AVACACHE_CACHE_DIR ??
    path.join(os.homedir(), '.cache', 'avacache', 'v1');
  const dir = path.join(root, String(chainId));
  await fs.mkdir(dir, { recursive: true });

  const safe = (key: string) => key.replace(/\//g, '__');

  return {
    async get(key: string): Promise<Uint8Array | null> {
      try {
        const buf = await fs.readFile(path.join(dir, safe(key)));
        return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
      } catch {
        return null;
      }
    },
    async put(key: string, bytes: Uint8Array): Promise<void> {
      const final = path.join(dir, safe(key));
      const tmp = `${final}.tmp`;
      await fs.writeFile(tmp, bytes);
      await fs.rename(tmp, final);
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
          const v = req.result as ArrayBuffer | undefined;
          resolve(v ? new Uint8Array(v) : null);
        };
        req.onerror = () => reject(req.error);
      });
    },
    async put(key: string, bytes: Uint8Array): Promise<void> {
      const db = await open();
      return new Promise((resolve, reject) => {
        const tx = db.transaction(store, 'readwrite');
        tx.objectStore(store).put(bytes.buffer, key);
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
