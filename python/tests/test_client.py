"""Client tests — no network; respx mocks the archive host."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import httpx
import pyarrow as pa
import pytest
import respx

from avacache import Client, Manifest


def _make(base: str, tmp_cache, **kw) -> Client:
    return Client(base_url=base, cache_dir=tmp_cache, offline=False, **kw)


@respx.mock
def test_manifest_fetched_and_cached(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    m = c.manifest()
    assert isinstance(m, Manifest)
    assert m.chain_id == 43114
    assert m.latest_complete_date == "2026-04-18"
    assert respx.calls.call_count == 1

    # Second call within TTL must not re-fetch.
    c.manifest()
    assert respx.calls.call_count == 1


@respx.mock
def test_load_day_decodes_hex(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)

    blocks = c.load_day("2026-04-18", "blocks")
    assert blocks.schema.field("base_fee_per_gas").type == pa.decimal128(38, 0)
    vals = blocks.column("base_fee_per_gas").to_pylist()
    assert vals == [Decimal(1), Decimal(42), None]

    txs = c.load_day("2026-04-18", "txs")
    assert txs.column("value").to_pylist()[1] == Decimal("1000000000000000000")

    events = c.load_day("2026-04-18", "events")
    assert events.schema.field("log_index").type == pa.int64()
    assert events.column("log_index").to_pylist() == [0, 1, 0]


@respx.mock
def test_load_day_disable_decode(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache, decode_hex=False)
    blocks = c.load_day("2026-04-18", "blocks")
    assert blocks.schema.field("base_fee_per_gas").type == pa.large_string()


@respx.mock
def test_cache_hit_skips_network(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    c.load_day("2026-04-18", "txs")
    n = respx.calls.call_count

    c2 = _make(mock_archive, tmp_cache)
    c2.load_day("2026-04-18", "txs")
    # Manifest was re-fetched (new process, empty TTL cache) but parquet wasn't.
    parquet_calls = [
        call for call in respx.calls
        if call.request.url.path.endswith(".parquet")
    ]
    assert len(parquet_calls) == 1


@respx.mock
def test_md5_mismatch_raises(mock_archive, tmp_cache, manifest_dict):
    # Corrupt manifest md5 so downloaded bytes don't match.
    manifest_dict["files"][0]["md5"] = "0" * 32
    # Rewire manifest route with the corrupted body.
    respx.get(f"{mock_archive}/manifest.json").mock(
        return_value=httpx.Response(200, json=manifest_dict)
    )
    c = _make(mock_archive, tmp_cache)
    with pytest.raises(ValueError, match="MD5 mismatch"):
        c.load_day("2026-04-18", "blocks")


@respx.mock
def test_missing_day_raises(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    with pytest.raises(FileNotFoundError):
        c.load_day("1999-01-01", "txs")


@respx.mock
def test_load_range_concatenates(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    # Range spans 3 days but only 2026-04-18 exists — other days silently skipped.
    t = c.load_range("2026-04-17", "2026-04-19", "blocks")
    assert t.num_rows == 3  # from the single real day


@respx.mock
def test_load_range_empty_raises(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    with pytest.raises(FileNotFoundError):
        c.load_range("1999-01-01", "1999-01-03", "blocks")


@respx.mock
def test_url_for(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    assert c.url_for("2026-04-18", "txs") == f"{mock_archive}/daily/2026-04-18.txs.parquet"


@respx.mock
def test_available_dates(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    assert c.available_dates("txs") == [date(2026, 4, 18)]


@respx.mock
def test_latest_complete_date(mock_archive, tmp_cache):
    c = _make(mock_archive, tmp_cache)
    assert c.latest_complete_date() == date(2026, 4, 18)
