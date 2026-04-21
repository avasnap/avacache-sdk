import { z } from 'zod';

export const KindSchema = z.enum(['blocks', 'txs', 'events']);
export type Kind = z.infer<typeof KindSchema>;

export const FileEntrySchema = z.object({
  date: z.string(),
  kind: KindSchema,
  key: z.string(),
  size: z.number().int().nonnegative(),
  md5: z.string(),
  schema_version: z.string(),
});
export type FileEntry = z.infer<typeof FileEntrySchema>;

export const LookupEntrySchema = z.object({
  key: z.string(),
  size: z.number().int().nonnegative(),
  md5: z.string(),
});
export type LookupEntry = z.infer<typeof LookupEntrySchema>;

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
