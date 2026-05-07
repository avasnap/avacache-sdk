"""Manifest model — the single source of truth published at bucket root."""

from __future__ import annotations

import re
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, field_validator

Kind = Literal["blocks", "txs", "events"]

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MD5_RE = re.compile(r"^[a-f0-9]{32}$")
_FILE_KEY_RE = re.compile(
    r"^daily/\d{4}-\d{2}-\d{2}\.(blocks|txs|events)\.parquet$"
)
_LOOKUP_KEY_RE = re.compile(r"^lookups/[A-Za-z0-9_-]+\.(json|json\.gz|parquet)$")


class FileEntry(BaseModel):
    date: str  # YYYY-MM-DD
    kind: Kind
    key: str
    size: int
    md5: str
    schema_version: str

    @field_validator("date")
    @classmethod
    def _check_date(cls, v: str) -> str:
        if not _DATE_RE.match(v):
            raise ValueError(f"invalid date {v!r}: expected YYYY-MM-DD")
        return v

    @field_validator("md5")
    @classmethod
    def _check_md5(cls, v: str) -> str:
        if not _MD5_RE.match(v):
            raise ValueError(f"invalid md5 {v!r}: expected 32 lowercase hex chars")
        return v

    @field_validator("size")
    @classmethod
    def _check_size(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"invalid size {v}: must be non-negative")
        return v

    @field_validator("key")
    @classmethod
    def _check_key(cls, v: str) -> str:
        if not _FILE_KEY_RE.match(v):
            raise ValueError(
                f"unsafe FileEntry key {v!r}: expected daily/<YYYY-MM-DD>.<kind>.parquet"
            )
        return v


class LookupEntry(BaseModel):
    key: str
    size: int
    md5: str

    @field_validator("md5")
    @classmethod
    def _check_md5(cls, v: str) -> str:
        if not _MD5_RE.match(v):
            raise ValueError(f"invalid md5 {v!r}: expected 32 lowercase hex chars")
        return v

    @field_validator("size")
    @classmethod
    def _check_size(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"invalid size {v}: must be non-negative")
        return v

    @field_validator("key")
    @classmethod
    def _check_key(cls, v: str) -> str:
        if not _LOOKUP_KEY_RE.match(v):
            raise ValueError(
                f"unsafe LookupEntry key {v!r}: expected lookups/<name>.<ext>"
            )
        return v


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
