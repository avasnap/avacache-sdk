/**
 * Optional DuckDB-WASM helper. Only import from `'avacache/duckdb'` when
 * you actually plan to run SQL over the archive.
 *
 *   import { openDay } from 'avacache/duckdb';
 *   const con = await openDay('2026-04-18');
 *   console.log(await con.query('SELECT COUNT(*) FROM txs WHERE status = 0'));
 */

import type * as duckdb from '@duckdb/duckdb-wasm';
import { Client, type ClientOptions } from './client.js';
import type { Kind } from './manifest.js';

export interface OpenOptions extends ClientOptions {
  /** Pre-initialized DuckDB instance. If omitted, one is lazily created. */
  db?: duckdb.AsyncDuckDB;
}

async function registerTable(
  db: duckdb.AsyncDuckDB,
  con: duckdb.AsyncDuckDBConnection,
  view: string,
  client: Client,
  date: string,
  kind: Kind,
): Promise<void> {
  const url = client.urlFor(date, kind);
  const fileName = `${view}.parquet`;
  await db.registerFileURL(fileName, url, 4 /* DuckDBDataProtocol.HTTP */, false);
  await con.query(
    `CREATE VIEW ${view} AS SELECT * FROM parquet_scan('${fileName}')`,
  );
}

export async function openDay(
  date: string,
  opts: OpenOptions = {},
): Promise<duckdb.AsyncDuckDBConnection> {
  const client = new Client(opts);
  const db = opts.db ?? (await defaultDuckDb());
  const con = await db.connect();

  for (const kind of ['blocks', 'txs', 'events'] as const) {
    await registerTable(db, con, kind, client, date, kind);
  }
  return con;
}

async function defaultDuckDb(): Promise<duckdb.AsyncDuckDB> {
  const mod = (await import('@duckdb/duckdb-wasm')) as typeof duckdb;
  const bundles = mod.getJsDelivrBundles();
  const bundle = await mod.selectBundle(bundles);
  const worker = new Worker(bundle.mainWorker!, { type: 'module' });
  const logger = new mod.ConsoleLogger();
  const db = new mod.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}
