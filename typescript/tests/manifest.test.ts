import { describe, expect, it } from 'vitest';
import {
  ManifestSchema,
  manifestDates,
  manifestEntry,
} from '../src/manifest.js';

const sample = {
  chain_id: 43114,
  generated_at: '2026-04-19T00:00:00Z',
  schema_version: 'v2',
  latest_complete_date: '2026-04-18',
  columns: { blocks: ['number'], txs: ['block_number'], events: [] },
  lookups: {},
  files: [
    {
      date: '2026-04-17',
      kind: 'txs',
      key: 'daily/2026-04-17.txs.parquet',
      size: 1,
      md5: 'a',
      schema_version: 'v2',
    },
    {
      date: '2026-04-18',
      kind: 'txs',
      key: 'daily/2026-04-18.txs.parquet',
      size: 2,
      md5: 'b',
      schema_version: 'v2',
    },
  ],
};

describe('manifest', () => {
  it('parses a minimal manifest', () => {
    const m = ManifestSchema.parse(sample);
    expect(m.chain_id).toBe(43114);
    expect(m.files).toHaveLength(2);
  });

  it('lists dates for a kind sorted', () => {
    const m = ManifestSchema.parse(sample);
    expect(manifestDates(m, 'txs')).toEqual(['2026-04-17', '2026-04-18']);
    expect(manifestDates(m, 'blocks')).toEqual([]);
  });

  it('looks up a specific entry', () => {
    const m = ManifestSchema.parse(sample);
    expect(manifestEntry(m, '2026-04-18', 'txs')?.md5).toBe('b');
    expect(manifestEntry(m, '1999-01-01', 'txs')).toBeUndefined();
  });

  it('rejects unknown kinds', () => {
    const bad = { ...sample, files: [{ ...sample.files[0], kind: 'wat' }] };
    expect(() => ManifestSchema.parse(bad)).toThrow();
  });
});
