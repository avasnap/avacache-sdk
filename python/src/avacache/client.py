"""Client for the public avacache parquet archive."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Literal
from urllib.parse import urlparse

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from avacache.hex import decode_hex_columns
from avacache.manifest import Manifest


def _maybe_pbar(total: int, enabled: bool | None, desc: str):
    """Return a tqdm progress bar, or a no-op stub.

    enabled=None auto-detects: on in notebooks, off otherwise. Silently
    falls back to a no-op stub if tqdm is not installed.
    """
    if enabled is None:
        try:
            from IPython import get_ipython  # type: ignore
            enabled = get_ipython() is not None
        except Exception:
            enabled = False
    if not enabled:
        class _Noop:
            def update(self, n: int = 1) -> None: ...
            def close(self) -> None: ...
        return _Noop()
    try:
        from tqdm.auto import tqdm  # type: ignore
        return tqdm(total=total, desc=desc, unit="file")
    except ImportError:
        class _Noop:
            def update(self, n: int = 1) -> None: ...
            def close(self) -> None: ...
        return _Noop()

Kind = Literal["blocks", "txs", "events"]

DEFAULT_BASE_URL = "https://parquet.avacache.com"
DEFAULT_CHAIN_ID = 43114
MANIFEST_TTL_SEC = 300
MANIFEST_MAX_BYTES = 16 * 1024 * 1024  # 16 MiB cap for manifest body


def _parse_date(d: str | date) -> date:
    return d if isinstance(d, date) else date.fromisoformat(d)


def _cache_root() -> Path:
    env = os.environ.get("AVACACHE_CACHE_DIR")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "avacache" / "v1"


def _offline_default() -> bool:
    return os.environ.get("AVACACHE_OFFLINE", "").lower() in ("1", "true", "yes")


def _validate_base_url(url: str) -> str:
    """Reject base URLs that aren't http(s). file://, gopher://, etc. are out."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"AVACACHE base_url must be http(s), got scheme {parsed.scheme!r} in {url!r}"
        )
    if not parsed.netloc:
        raise ValueError(f"AVACACHE base_url is missing host: {url!r}")
    return url


def _md5_file(path: Path, chunk_size: int = 1 << 20) -> str:
    """Stream-hash a file with md5. One read pass, OS page cache-friendly."""
    h = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    """Write text via tmp+rename so a crash mid-write can't corrupt the target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}-{secrets.token_hex(4)}.tmp")
    try:
        tmp.write_text(text)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


class Client:
    """Load daily parquet files from the public avacache archive.

    >>> c = Client()
    >>> t = c.load_day("2026-04-18", "txs")
    >>> t.num_rows
    2732837
    """

    def __init__(
        self,
        chain_id: int = DEFAULT_CHAIN_ID,
        base_url: str | None = None,
        cache_dir: Path | str | None = None,
        offline: bool | None = None,
        timeout: float = 60.0,
        decode_hex: bool = True,
    ):
        self.chain_id = chain_id
        raw_base = (
            base_url
            or os.environ.get("AVACACHE_BASE_URL")
            or DEFAULT_BASE_URL
        ).rstrip("/")
        self.base_url = _validate_base_url(raw_base)
        self.cache_dir = Path(cache_dir) if cache_dir else _cache_root()
        self.offline = offline if offline is not None else _offline_default()
        self.decode_hex = decode_hex

        # follow_redirects=False: the archive is a flat bucket, redirects would
        # mean the origin is misconfigured or compromised. http2 is off because
        # h2 isn't a declared dep.
        self._http = httpx.Client(timeout=timeout, http2=False, follow_redirects=False)
        self._manifest: Manifest | None = None
        self._manifest_at: float = 0.0
        # md5s already verified on disk this process — avoids re-hashing on
        # every load_day call within a long-lived Client.
        self._verified: set[str] = set()

    # ---- URLs -------------------------------------------------------------

    def url_for(self, d: str | date, kind: Kind) -> str:
        ds = _parse_date(d).isoformat()
        return f"{self.base_url}/daily/{ds}.{kind}.parquet"

    def manifest_url(self) -> str:
        return f"{self.base_url}/manifest.json"

    # ---- Manifest ---------------------------------------------------------

    def manifest(self, force: bool = False) -> Manifest:
        now = time.time()
        if (
            not force
            and self._manifest is not None
            and (now - self._manifest_at) < MANIFEST_TTL_SEC
        ):
            return self._manifest

        # On disk: cache the manifest JSON too (for offline)
        cache_path = self._manifest_cache_path()

        if self.offline:
            if not cache_path.exists():
                raise RuntimeError(
                    "AVACACHE_OFFLINE=1 but no cached manifest at "
                    f"{cache_path}"
                )
            data = json.loads(cache_path.read_text())
        else:
            data = self._fetch_manifest_json()
            _atomic_write_text(cache_path, json.dumps(data))

        manifest = Manifest.model_validate(data)
        if manifest.chain_id != self.chain_id:
            raise ValueError(
                f"manifest chain_id {manifest.chain_id} does not match "
                f"configured chain_id {self.chain_id}"
            )
        self._manifest = manifest
        self._manifest_at = now
        return self._manifest

    def _fetch_manifest_json(self) -> dict:
        """Stream the manifest with a hard size cap to prevent OOM."""
        with self._http.stream("GET", self.manifest_url()) as resp:
            if resp.status_code != 200:
                raise httpx.HTTPStatusError(
                    f"manifest fetch returned {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
            body = bytearray()
            for chunk in resp.iter_bytes(chunk_size=64 << 10):
                body += chunk
                if len(body) > MANIFEST_MAX_BYTES:
                    raise ValueError(
                        f"manifest body exceeds {MANIFEST_MAX_BYTES} bytes — "
                        "refusing to buffer further"
                    )
        return json.loads(bytes(body))

    def available_dates(self, kind: Kind) -> list[date]:
        return self.manifest().dates(kind)

    def latest_complete_date(self) -> date | None:
        s = self.manifest().latest_complete_date
        return date.fromisoformat(s) if s else None

    # ---- Single day -------------------------------------------------------

    def load_day(self, d: str | date, kind: Kind) -> pa.Table:
        entry = self.manifest().entry(d, kind)
        if entry is None:
            raise FileNotFoundError(
                f"No {kind} parquet for {d!s} in manifest "
                f"(latest_complete_date={self.latest_complete_date()})"
            )

        path = self._fetch_to_cache(entry.key, entry.md5, entry.size)
        table = pq.read_table(path)
        if self.decode_hex:
            table = decode_hex_columns(table, kind)
        return table

    def _range_tasks(
        self, start: str | date, end: str | date, kind: Kind,
    ) -> list[tuple[date, object]]:
        s, e = _parse_date(start), _parse_date(end)
        if e < s:
            raise ValueError("end before start")
        manifest = self.manifest()
        tasks: list[tuple[date, object]] = []
        d = s
        while d <= e:
            entry = manifest.entry(d, kind)
            if entry is not None:
                tasks.append((d, entry))
            d += timedelta(days=1)
        if not tasks:
            raise FileNotFoundError(f"No {kind} files in range {s}..{e}")
        return tasks

    def iter_range(
        self,
        start: str | date,
        end: str | date,
        kind: Kind,
        *,
        concurrency: int = 4,
        progress: bool | None = None,
    ) -> Iterator[tuple[date, pa.Table]]:
        """Yield (date, table) pairs one day at a time, in date order.

        Downloads up to `concurrency` files ahead while the consumer processes
        earlier days. Memory: one decoded table in flight. Prefer this over
        load_range() for multi-month ranges.
        """
        tasks = self._range_tasks(start, end, kind)
        s, e = tasks[0][0], tasks[-1][0]
        pbar = _maybe_pbar(len(tasks), progress, desc=f"{kind} {s}..{e}")
        try:
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futs = [
                    pool.submit(self._fetch_to_cache, t[1].key, t[1].md5, t[1].size)
                    for t in tasks
                ]
                for (day, _entry), fut in zip(tasks, futs):
                    path = fut.result()
                    table = pq.read_table(path)
                    if self.decode_hex:
                        table = decode_hex_columns(table, kind)
                    pbar.update(1)
                    yield day, table
        finally:
            pbar.close()

    def load_range(
        self,
        start: str | date,
        end: str | date,
        kind: Kind,
        *,
        concurrency: int = 8,
        progress: bool | None = None,
    ) -> pa.Table:
        """Load a date range as one concatenated pyarrow table.

        Downloads in parallel (concurrency threads). Shows a tqdm bar in
        notebooks by default — pass progress=True/False to force either way.

        Beware memory: all days are held in RAM at once. For multi-month
        ranges of txs or events, prefer iter_range() and process day-by-day.
        """
        tables = [t for _, t in self.iter_range(
            start, end, kind, concurrency=concurrency, progress=progress,
        )]
        return pa.concat_tables(tables, promote_options="default")

    # ---- Cache management -------------------------------------------------

    def _manifest_cache_path(self) -> Path:
        return self.cache_dir / str(self.chain_id) / "manifest.json"

    def _object_cache_path(self, key: str, md5: str) -> Path:
        # key is "daily/YYYY-MM-DD.kind.parquet"
        rel = Path(key)
        stem = rel.stem
        return (
            self.cache_dir
            / str(self.chain_id)
            / rel.parent
            / f"{stem}.{md5[:16]}.parquet"
        )

    def _fetch_to_cache(self, key: str, md5: str, size: int) -> Path:
        # key was validated by FileEntry / LookupEntry pydantic models.
        path = self._object_cache_path(key, md5)
        # Defense in depth: the resolved cache path must stay within
        # cache_dir/<chain_id>/. With key already regex-validated this is
        # belt-and-braces, but cheap.
        chain_root = (self.cache_dir / str(self.chain_id)).resolve()
        if chain_root != path.resolve().parents[0] and chain_root not in path.resolve().parents:
            raise ValueError(f"resolved cache path escapes chain root: {path}")

        if path.exists() and path.stat().st_size == size:
            # Cache hit: re-verify md5 once per process, then trust.
            # The md5-suffixed filename means a stale file is unlikely, but
            # truncation, partial writes from another process, or fs
            # corruption can still leave a same-size-different-bytes file.
            if md5 in self._verified or _md5_file(path) == md5:
                self._verified.add(md5)
                return path
            # Mismatch: drop and re-download.
            path.unlink(missing_ok=True)

        if self.offline:
            raise RuntimeError(
                f"AVACACHE_OFFLINE=1 but {path} is not cached"
            )

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(
            f"{path.name}.{os.getpid()}-{secrets.token_hex(4)}.tmp"
        )

        # O_EXCL prevents two writers from racing on the same tmp; O_NOFOLLOW
        # neutralizes a pre-placed symlink in a shared cache dir. The random
        # suffix makes collisions effectively impossible.
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC

        url = f"{self.base_url}/{key}"
        try:
            with self._http.stream("GET", url) as resp:
                if resp.status_code != 200:
                    raise httpx.HTTPStatusError(
                        f"object fetch returned {resp.status_code} for {key}",
                        request=resp.request,
                        response=resp,
                    )
                hasher = hashlib.md5()
                running = 0
                fd = os.open(tmp, flags, 0o600)
                try:
                    with os.fdopen(fd, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=1 << 20):
                            running += len(chunk)
                            if running > size:
                                raise ValueError(
                                    f"oversized download for {key}: "
                                    f"received >{size} bytes, manifest declared {size}"
                                )
                            hasher.update(chunk)
                            f.write(chunk)
                finally:
                    # If os.fdopen raised before consuming fd, close it.
                    try:
                        os.close(fd)
                    except OSError:
                        pass

            if running != size:
                raise ValueError(
                    f"size mismatch for {key}: got {running} bytes, "
                    f"expected {size}"
                )
            got = hasher.hexdigest()
            if got != md5:
                raise ValueError(
                    f"MD5 mismatch downloading {key}: "
                    f"expected {md5}, got {got}"
                )
            tmp.replace(path)
            self._verified.add(md5)
            return path
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def prune_cache(self, max_gb: float = 50.0) -> int:
        """Delete least-recently-accessed cache files beyond `max_gb`."""
        root = self.cache_dir
        if not root.exists():
            return 0
        files = [p for p in root.rglob("*.parquet") if p.is_file()]
        files.sort(key=lambda p: p.stat().st_atime)
        total = sum(p.stat().st_size for p in files)
        budget = int(max_gb * 1_073_741_824)
        removed = 0
        for p in files:
            if total <= budget:
                break
            sz = p.stat().st_size
            p.unlink()
            total -= sz
            removed += 1
        return removed

    # ---- Housekeeping ----------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
