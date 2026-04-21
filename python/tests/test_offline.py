"""AVACACHE_OFFLINE=1 serves only from the local cache."""

from __future__ import annotations

import json

import pytest
import respx

from avacache import Client


@respx.mock
def test_offline_without_cache_raises(mock_archive, tmp_cache):
    c = Client(base_url=mock_archive, cache_dir=tmp_cache, offline=True)
    with pytest.raises(RuntimeError, match="no cached manifest"):
        c.manifest()


@respx.mock
def test_offline_served_from_cache(mock_archive, tmp_cache):
    # First, warm the cache online.
    online = Client(base_url=mock_archive, cache_dir=tmp_cache, offline=False)
    online.load_day("2026-04-18", "txs")

    # Then go offline — no network calls should be made.
    respx.reset()
    offline = Client(base_url=mock_archive, cache_dir=tmp_cache, offline=True)
    t = offline.load_day("2026-04-18", "txs")
    assert t.num_rows == 2
    assert len(respx.calls) == 0


@respx.mock
def test_offline_env_var(monkeypatch, mock_archive, tmp_cache):
    monkeypatch.setenv("AVACACHE_OFFLINE", "1")
    c = Client(base_url=mock_archive, cache_dir=tmp_cache)
    assert c.offline is True
