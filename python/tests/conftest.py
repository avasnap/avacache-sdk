"""Shared fixtures: a tiny in-memory parquet archive + matching manifest."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


SAMPLE_SELECTORS = {
    "0xa9059cbb": [
        {"signature": "transfer(address,uint256)", "count": 491287, "abi": {"name": "transfer"}},
        {"signature": "transfer(bytes4,bytes32)", "count": 12, "abi": {"name": "transfer"}},
    ],
    "0x70a08231": [
        {"signature": "balanceOf(address)", "count": 88000, "abi": {"name": "balanceOf"}},
    ],
}

SAMPLE_TOPICS = {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": [
        {"signature": "Transfer(address,address,uint256)", "count": 603616, "abi": {"name": "Transfer"}},
    ],
}


def _build_parquet(kind: str) -> bytes:
    if kind == "blocks":
        table = pa.table({
            "number":           pa.array([1, 2, 3], type=pa.int64()),
            "timestamp":        pa.array([1700000000, 1700000001, 1700000002], type=pa.int64()),
            "gas_limit":        pa.array([15_000_000] * 3, type=pa.int64()),
            "gas_used":         pa.array([100, 200, 300], type=pa.int64()),
            "base_fee_per_gas": pa.array(["0x1", "0x2a", None], type=pa.large_string()),
        })
    elif kind == "txs":
        table = pa.table({
            "block_number":             pa.array([1, 2], type=pa.int64()),
            "tx_hash":                  pa.array(["0xaa", "0xbb"], type=pa.large_string()),
            "from_address":             pa.array(["0x11", "0x22"], type=pa.large_string()),
            "to_address":               pa.array(["0x33", None], type=pa.large_string()),
            "value":                    pa.array(["0x0", "0xde0b6b3a7640000"], type=pa.large_string()),
            "gas_used":                 pa.array([21000, 90000], type=pa.int64()),
            "effective_gas_price":      pa.array(["0x1", "0x2"], type=pa.large_string()),
            "gas_price":                pa.array(["0x1", "0x2"], type=pa.large_string()),
            "max_priority_fee_per_gas": pa.array(["0x1", None], type=pa.large_string()),
            "type":                     pa.array([0, 2], type=pa.int64()),
            "status":                   pa.array([1, 1], type=pa.int64()),
            "input_prefix":             pa.array(["0x", "0xa9059cbb"], type=pa.large_string()),
        })
    elif kind == "events":
        table = pa.table({
            "block_number": pa.array([1, 1, 2], type=pa.int64()),
            "tx_hash":      pa.array(["0xaa", "0xaa", "0xbb"], type=pa.large_string()),
            "log_index":    pa.array(["0x0", "0x1", "0x0"], type=pa.large_string()),
            "address":      pa.array(["0x11", "0x22", "0x33"], type=pa.large_string()),
            "topic0":       pa.array(["0xddf"] * 3, type=pa.large_string()),
            "topic1":       pa.array([None] * 3, type=pa.large_string()),
            "topic2":       pa.array([None] * 3, type=pa.large_string()),
            "topic3":       pa.array([None] * 3, type=pa.large_string()),
            "data":         pa.array([None, "0xbeef", None], type=pa.large_string()),
        })
    else:
        raise ValueError(kind)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", compression_level=3)
    return buf.getvalue()


@pytest.fixture
def archive_bytes() -> dict[str, bytes]:
    """Parquet bodies for one day, one of each kind."""
    return {kind: _build_parquet(kind) for kind in ("blocks", "txs", "events")}


@pytest.fixture
def lookup_bodies() -> dict[str, bytes]:
    """Canonical JSON bodies for the published lookup files."""
    return {
        "function_selectors": json.dumps(SAMPLE_SELECTORS).encode(),
        "event_topics": json.dumps(SAMPLE_TOPICS).encode(),
    }


@pytest.fixture
def manifest_dict(archive_bytes, lookup_bodies) -> dict:
    files = []
    for kind, body in archive_bytes.items():
        files.append({
            "date": "2026-04-18",
            "kind": kind,
            "key": f"daily/2026-04-18.{kind}.parquet",
            "size": len(body),
            "md5": hashlib.md5(body).hexdigest(),
            "schema_version": "v2",
        })
    lookups = {
        "function_selectors": {
            "key": "lookups/function_selectors.json",
            "size": len(lookup_bodies["function_selectors"]),
            "md5": hashlib.md5(lookup_bodies["function_selectors"]).hexdigest(),
        },
        "event_topics": {
            "key": "lookups/event_topics.json",
            "size": len(lookup_bodies["event_topics"]),
            "md5": hashlib.md5(lookup_bodies["event_topics"]).hexdigest(),
        },
    }
    return {
        "chain_id": 43114,
        "generated_at": "2026-04-19T00:00:00Z",
        "schema_version": "v2",
        "latest_complete_date": "2026-04-18",
        "columns": {
            "blocks": ["number", "timestamp", "gas_limit", "gas_used", "base_fee_per_gas"],
            "txs":    ["block_number", "tx_hash"],
            "events": ["block_number", "tx_hash", "log_index"],
        },
        "lookups": lookups,
        "files": files,
    }


@pytest.fixture
def mock_archive(respx_mock, archive_bytes, manifest_dict, lookup_bodies):
    """Wire respx routes for manifest, every daily parquet, and lookup files."""
    from httpx import Response

    base = "https://cache.example.test"
    respx_mock.get(f"{base}/manifest.json").mock(
        return_value=Response(200, json=manifest_dict)
    )
    for kind, body in archive_bytes.items():
        respx_mock.get(f"{base}/daily/2026-04-18.{kind}.parquet").mock(
            return_value=Response(200, content=body)
        )
    for name, body in lookup_bodies.items():
        respx_mock.get(f"{base}/lookups/{name}.json").mock(
            return_value=Response(200, content=body)
        )
    return base


@pytest.fixture
def tmp_cache(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("AVACACHE_CACHE_DIR", str(tmp_path))
    return tmp_path
