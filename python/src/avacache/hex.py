"""Vectorized decoding of hex-encoded numeric columns.

The cache stores wei-scale integers as 0x-prefixed hex strings because
uint256 doesn't fit in int64. Some smaller integers (e.g. event log_index)
are also hex-encoded upstream. This module decodes both groups:

- Wei columns → DECIMAL(38,0)
- Small-int columns (log_index) → int64

Pyarrow has no native hex kernel, so we pay one Python loop per column
per load — the Decimal / int constructors are implemented in C.
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa

# uint256 wei values — don't fit in int64, decoded to DECIMAL(38,0).
TX_WEI_COLS:    tuple[str, ...] = (
    "value", "gas_price", "effective_gas_price", "max_priority_fee_per_gas",
)
BLOCK_WEI_COLS: tuple[str, ...] = ("base_fee_per_gas",)
EVENT_WEI_COLS: tuple[str, ...] = ()

# Small integers stored as hex — fit comfortably in int64.
TX_INT_COLS:    tuple[str, ...] = ()
BLOCK_INT_COLS: tuple[str, ...] = ()
EVENT_INT_COLS: tuple[str, ...] = ("log_index",)

DECIMAL_TYPE = pa.decimal128(38, 0)


def _decode_wei(val: str | None) -> Decimal | None:
    if val is None or val == "":
        return None
    if val == "0x":
        return Decimal(0)
    try:
        return Decimal(int(val, 16))
    except (ValueError, TypeError):
        return None


def _decode_int(val: str | None) -> int | None:
    if val is None or val == "" or val == "0x":
        return 0 if val == "0x" else None
    try:
        return int(val, 16)
    except (ValueError, TypeError):
        return None


def _decode_array(arr: pa.Array | pa.ChunkedArray, as_int: bool) -> pa.Array:
    values = arr.to_pylist()
    if as_int:
        return pa.array([_decode_int(s) for s in values], type=pa.int64())
    return pa.array([_decode_wei(s) for s in values], type=DECIMAL_TYPE)


def decode_hex_columns(table: pa.Table, kind: str) -> pa.Table:
    """Return a new table with known hex columns cast to numeric types.

    Wei columns become DECIMAL(38,0); small-int columns become int64.
    Columns absent from the schema are silently skipped (forward
    compatibility with older files).
    """
    wei_cols = {
        "blocks": BLOCK_WEI_COLS,
        "txs":    TX_WEI_COLS,
        "events": EVENT_WEI_COLS,
    }.get(kind, ())
    int_cols = {
        "blocks": BLOCK_INT_COLS,
        "txs":    TX_INT_COLS,
        "events": EVENT_INT_COLS,
    }.get(kind, ())

    for name in wei_cols:
        if name in table.schema.names:
            idx = table.schema.get_field_index(name)
            table = table.set_column(idx, name, _decode_array(table.column(name), as_int=False))
    for name in int_cols:
        if name in table.schema.names:
            idx = table.schema.get_field_index(name)
            table = table.set_column(idx, name, _decode_array(table.column(name), as_int=True))
    return table
