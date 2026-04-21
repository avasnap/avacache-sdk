"""Hits the real R2-backed archive. Run with: pytest -m integration"""

from __future__ import annotations

import pytest

from avacache import Client

pytestmark = pytest.mark.integration


def test_manifest_online():
    c = Client()  # defaults to parquet.avacache.com
    m = c.manifest()
    assert m.chain_id == 43114
    assert m.latest_complete_date is not None
    assert len(m.files) > 0


def test_load_real_day():
    c = Client()
    d = c.latest_complete_date()
    assert d is not None
    t = c.load_day(d, "txs")
    assert t.num_rows > 0
    assert "tx_hash" in t.schema.names
