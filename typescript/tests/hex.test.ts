import { describe, expect, it } from 'vitest';
import { decodeRow } from '../src/hex.js';

describe('decodeRow', () => {
  it('converts wei columns to bigint on txs', () => {
    const row = {
      value: '0xde0b6b3a7640000',
      gas_price: '0x1',
      effective_gas_price: '0x2',
      max_priority_fee_per_gas: null,
      status: 1,
    };
    decodeRow(row, 'txs');
    expect(row.value).toBe(1_000_000_000_000_000_000n);
    expect(row.gas_price).toBe(1n);
    expect(row.effective_gas_price).toBe(2n);
    expect(row.max_priority_fee_per_gas).toBeNull();
    expect(row.status).toBe(1);
  });

  it('converts base_fee on blocks and handles 0x', () => {
    const row = { base_fee_per_gas: '0x', gas_used: 1 };
    decodeRow(row, 'blocks');
    expect(row.base_fee_per_gas).toBe(0n);
    expect(row.gas_used).toBe(1);
  });

  it('converts log_index to number on events', () => {
    const row = {
      log_index: '0x2a',
      topic0: '0xddf...',
    };
    decodeRow(row, 'events');
    expect(row.log_index).toBe(42);
    expect(row.topic0).toBe('0xddf...'); // strings preserved
  });

  it('tolerates missing columns', () => {
    const row = { irrelevant: 'x' };
    expect(() => decodeRow(row, 'txs')).not.toThrow();
    expect(row.irrelevant).toBe('x');
  });
});
