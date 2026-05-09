"""CLI tests — exercise every subcommand against the respx-mocked archive."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import respx

from avacache.cli import main


def _run(argv, base, tmp_cache, capsys):
    rc = main([
        "--base-url", base,
        "--cache-dir", str(tmp_cache),
        *argv,
    ])
    out, err = capsys.readouterr()
    return rc, out, err


@respx.mock
def test_manifest_summary(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(["manifest"], mock_archive, tmp_cache, capsys)
    assert rc == 0
    assert "chain_id:             43114" in out
    assert "latest_complete_date: 2026-04-18" in out
    assert "blocks  1 files" in out
    assert "function_selectors" in out


@respx.mock
def test_manifest_json_is_valid(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(["manifest", "--json"], mock_archive, tmp_cache, capsys)
    assert rc == 0
    parsed = json.loads(out)
    assert parsed["chain_id"] == 43114
    assert parsed["latest_complete_date"] == "2026-04-18"


@respx.mock
def test_latest(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(["latest"], mock_archive, tmp_cache, capsys)
    assert rc == 0
    assert out.strip() == "2026-04-18"


@respx.mock
def test_dates(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(["dates", "--kind", "txs"], mock_archive, tmp_cache, capsys)
    assert rc == 0
    assert out.strip() == "2026-04-18"


@respx.mock
def test_url(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(["url", "2026-04-18", "txs"], mock_archive, tmp_cache, capsys)
    assert rc == 0
    assert out.strip() == f"{mock_archive}/daily/2026-04-18.txs.parquet"


@respx.mock
def test_url_missing_returns_exit_2(mock_archive, tmp_cache, capsys):
    rc, _out, err = _run(["url", "1999-01-01", "txs"], mock_archive, tmp_cache, capsys)
    assert rc == 2
    assert "no txs parquet for 1999-01-01" in err


@respx.mock
def test_fetch_to_cache_path(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(["fetch", "2026-04-18", "blocks"], mock_archive, tmp_cache, capsys)
    assert rc == 0
    cached = Path(out.strip())
    assert cached.exists()
    assert cached.stat().st_size > 0


@respx.mock
def test_fetch_writes_to_out(mock_archive, tmp_cache, tmp_path, capsys):
    target = tmp_path / "blocks.parquet"
    rc, out, _ = _run(
        ["fetch", "2026-04-18", "blocks", "--out", str(target)],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    assert out.strip() == str(target)
    assert target.exists()
    assert target.stat().st_size > 0


@respx.mock
def test_show_ndjson_decodes_hex(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(
        ["show", "2026-04-18", "txs", "--format", "ndjson", "--limit", "0"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    rows = [json.loads(line) for line in out.strip().splitlines()]
    assert len(rows) == 2
    # Decimal values are emitted as strings via the json default hook.
    assert rows[0]["value"] == "0"
    assert rows[1]["value"] == "1000000000000000000"
    assert rows[0]["status"] == 1


@respx.mock
def test_show_csv(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(
        ["show", "2026-04-18", "blocks", "--format", "csv", "--limit", "0"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    lines = out.strip().splitlines()
    assert lines[0].startswith("number,timestamp")
    # 3 data rows
    assert len(lines) == 4


@respx.mock
def test_show_no_decode_hex_keeps_raw(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(
        ["--no-decode-hex", "show", "2026-04-18", "txs",
         "--format", "ndjson", "--limit", "1"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    row = json.loads(out.strip().splitlines()[0])
    assert row["value"] == "0x0"  # raw upstream string, not decoded


@respx.mock
def test_lookup_selector_sorted_by_count(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(
        ["lookup", "selector", "0xa9059cbb"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    lines = out.strip().splitlines()
    # Most common signature first.
    assert lines[0] == "transfer(address,uint256)\t491287"
    assert lines[1] == "transfer(bytes4,bytes32)\t12"


@respx.mock
def test_lookup_selector_uppercase_key(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(
        ["lookup", "selector", "0xA9059CBB"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    assert "transfer(address,uint256)" in out


@respx.mock
def test_lookup_topic_json(mock_archive, tmp_cache, capsys):
    rc, out, _ = _run(
        ["lookup", "topic",
         "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
         "--json"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    parsed = json.loads(out)
    assert isinstance(parsed, list)
    assert parsed[0]["signature"] == "Transfer(address,address,uint256)"
    assert parsed[0]["count"] == 603616


@respx.mock
def test_lookup_unknown_returns_1(mock_archive, tmp_cache, capsys):
    rc, _out, err = _run(
        ["lookup", "selector", "0xdeadbeef"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 1
    assert "no entry for 0xdeadbeef" in err


@respx.mock
def test_lookup_streaming_finds_key_across_tiny_chunks(
    respx_mock, mock_archive, tmp_cache, capsys, lookup_bodies, manifest_dict,
):
    """Force the streaming scanner to see one byte at a time so the
    bracket-walker has to handle keys, values, and string escapes that
    span chunk boundaries. Replaces the default mock with a chunked one."""
    from httpx import Response
    from httpx._content import IteratorByteStream

    body = lookup_bodies["function_selectors"]
    # 1-byte chunks — pathological, but proves correctness.
    chunks = [bytes([b]) for b in body]
    respx_mock.get(f"{mock_archive}/lookups/function_selectors.json").mock(
        return_value=Response(200, stream=IteratorByteStream(chunks))
    )
    rc, out, _ = _run(
        ["lookup", "selector", "0x70a08231"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    assert out.strip() == "balanceOf(address)\t88000"


@respx.mock
def test_lookup_streaming_first_key_with_trailing_data(
    respx_mock, mock_archive, tmp_cache, capsys, lookup_bodies, manifest_dict,
):
    """Regression: when the matched value's closing bracket is followed
    by more bytes (i.e. it's not the last key), the scanner must NOT
    keep appending those trailing bytes to the captured fragment.
    Earlier versions ignored ``consume_for_capture``'s True return and
    produced ``Extra data`` JSON parse failures on every multi-key
    file in practice."""
    from httpx import Response
    from httpx._content import IteratorByteStream

    body = lookup_bodies["function_selectors"]
    chunks = [bytes([b]) for b in body]
    respx_mock.get(f"{mock_archive}/lookups/function_selectors.json").mock(
        return_value=Response(200, stream=IteratorByteStream(chunks))
    )
    # 0xa9059cbb is the FIRST key — its closing `]` has many more bytes
    # after it (the second key entry plus the closing `}`).
    rc, out, _ = _run(
        ["lookup", "selector", "0xa9059cbb"],
        mock_archive, tmp_cache, capsys,
    )
    assert rc == 0
    lines = out.strip().splitlines()
    assert lines[0] == "transfer(address,uint256)\t491287"
    assert lines[1] == "transfer(bytes4,bytes32)\t12"


@respx.mock
def test_lookup_refuses_when_offline(mock_archive, tmp_cache, capsys):
    """`--offline` must refuse to hit the network for lookups, mirroring
    the manifest/parquet contract."""
    # Warm the manifest cache while online so the offline run gets past
    # the manifest fetch and reaches the lookup-specific offline guard.
    main([
        "--base-url", mock_archive,
        "--cache-dir", str(tmp_cache),
        "manifest",
    ])
    capsys.readouterr()  # discard manifest output
    with pytest.raises(RuntimeError, match="AVACACHE_OFFLINE"):
        main([
            "--base-url", mock_archive,
            "--cache-dir", str(tmp_cache),
            "--offline",
            "lookup", "selector", "0xa9059cbb",
        ])


@respx.mock
def test_lookup_md5_mismatch_raises(mock_archive, tmp_cache, capsys, manifest_dict):
    # Corrupt the manifest's md5 for function_selectors so the verify step trips.
    manifest_dict["lookups"]["function_selectors"]["md5"] = "0" * 32
    # Re-mock with the corrupted manifest.
    from httpx import Response
    respx.get(f"{mock_archive}/manifest.json").mock(
        return_value=Response(200, json=manifest_dict)
    )
    with pytest.raises(ValueError, match="md5 mismatch"):
        main([
            "--base-url", mock_archive,
            "--cache-dir", str(tmp_cache),
            "lookup", "selector", "0xa9059cbb",
        ])
