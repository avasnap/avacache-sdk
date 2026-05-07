import { z } from 'zod';

export const KindSchema = z.enum(['blocks', 'txs', 'events']);
/** Daily parquet category — one of `'blocks' | 'txs' | 'events'`. */
export type Kind = z.infer<typeof KindSchema>;

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const MD5_RE = /^[a-f0-9]{32}$/;
const FILE_KEY_RE = /^daily\/\d{4}-\d{2}-\d{2}\.(blocks|txs|events)\.parquet$/;
const LOOKUP_KEY_RE = /^lookups\/[A-Za-z0-9_-]+\.(json|json\.gz|parquet)$/;

const dateString = z.string().regex(DATE_RE, 'expected YYYY-MM-DD');
const md5String = z.string().regex(MD5_RE, 'expected 32 lowercase hex chars');

/** Manifest entry for one daily parquet. The `key` is relative to the archive root. */
export const FileEntrySchema = z.object({
  date: dateString,
  kind: KindSchema,
  key: z.string().regex(FILE_KEY_RE, 'unsafe FileEntry key'),
  size: z.number().int().nonnegative(),
  md5: md5String,
  schema_version: z.string(),
});
export type FileEntry = z.infer<typeof FileEntrySchema>;

/** Manifest entry for a lookup file (e.g. function selectors, event topics). */
export const LookupEntrySchema = z.object({
  key: z.string().regex(LOOKUP_KEY_RE, 'unsafe LookupEntry key'),
  size: z.number().int().nonnegative(),
  md5: md5String,
});
export type LookupEntry = z.infer<typeof LookupEntrySchema>;

/**
 * The archive's `manifest.json` shape — single source of truth for what
 * exists in the bucket. `latest_complete_date` is the latest day for
 * which all three kinds (`blocks`, `txs`, `events`) are published.
 */
export const ManifestSchema = z.object({
  chain_id: z.number().int(),
  generated_at: z.string(),
  schema_version: z.string(),
  latest_complete_date: z.string().nullable().optional(),
  columns: z.record(z.array(z.string())).default({}),
  lookups: z.record(LookupEntrySchema).default({}),
  files: z.array(FileEntrySchema).default([]),
});
export type Manifest = z.infer<typeof ManifestSchema>;

export function manifestDates(m: Manifest, kind: Kind): string[] {
  const s = new Set<string>();
  for (const f of m.files) if (f.kind === kind) s.add(f.date);
  return [...s].sort();
}

export function manifestEntry(
  m: Manifest,
  date: string,
  kind: Kind,
): FileEntry | undefined {
  return m.files.find((f) => f.date === date && f.kind === kind);
}
