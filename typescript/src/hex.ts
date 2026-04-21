import type { Kind } from './manifest.js';

// Wei-scale columns (uint256) → bigint.
const TX_WEI_COLS = [
  'value',
  'gas_price',
  'effective_gas_price',
  'max_priority_fee_per_gas',
] as const;
const BLOCK_WEI_COLS = ['base_fee_per_gas'] as const;
const EVENT_WEI_COLS: readonly string[] = [];

// Small-int hex columns → number.
const TX_INT_COLS: readonly string[] = [];
const BLOCK_INT_COLS: readonly string[] = [];
const EVENT_INT_COLS = ['log_index'] as const;

function hexToBigint(v: unknown): bigint | null {
  if (v === null || v === undefined || v === '') return null;
  if (v === '0x') return 0n;
  if (typeof v !== 'string') return null;
  try {
    return BigInt(v);
  } catch {
    return null;
  }
}

function hexToNumber(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null;
  if (v === '0x') return 0;
  if (typeof v !== 'string') return null;
  const n = Number.parseInt(v, 16);
  return Number.isFinite(n) ? n : null;
}

function colsForKind(kind: Kind): {
  wei: readonly string[];
  int: readonly string[];
} {
  switch (kind) {
    case 'blocks':
      return { wei: BLOCK_WEI_COLS, int: BLOCK_INT_COLS };
    case 'txs':
      return { wei: TX_WEI_COLS, int: TX_INT_COLS };
    case 'events':
      return { wei: EVENT_WEI_COLS, int: EVENT_INT_COLS };
  }
}

export type Row = Record<string, unknown>;

/** Mutates `row` in place, returning it. Converts known hex columns. */
export function decodeRow(row: Row, kind: Kind): Row {
  const { wei, int } = colsForKind(kind);
  for (const k of wei) if (k in row) row[k] = hexToBigint(row[k]);
  for (const k of int) if (k in row) row[k] = hexToNumber(row[k]);
  return row;
}
