"""Manifest model — the single source of truth published at bucket root."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field

Kind = Literal["blocks", "txs", "events"]


class FileEntry(BaseModel):
    date: str  # YYYY-MM-DD
    kind: Kind
    key: str
    size: int
    md5: str
    schema_version: str


class LookupEntry(BaseModel):
    key: str
    size: int
    md5: str


class Manifest(BaseModel):
    chain_id: int
    generated_at: str
    schema_version: str
    latest_complete_date: str | None = None
    columns: dict[str, list[str]] = Field(default_factory=dict)
    lookups: dict[str, LookupEntry] = Field(default_factory=dict)
    files: list[FileEntry] = Field(default_factory=list)

    def dates(self, kind: Kind) -> list[date]:
        return sorted({date.fromisoformat(f.date) for f in self.files
                       if f.kind == kind})

    def entry(self, d: str | date, kind: Kind) -> FileEntry | None:
        ds = d.isoformat() if isinstance(d, date) else d
        for f in self.files:
            if f.date == ds and f.kind == kind:
                return f
        return None
