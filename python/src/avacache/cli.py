"""avacache CLI — thin wrapper over the SDK for shell / agent use.

Stable contract is documented in .claude/skills/avacache/SKILL.md, not here.
Subcommands and flags can change between minor versions; consult the skill.
"""

from __future__ import annotations

import argparse
import csv
import decimal
import hashlib
import json
import sys
from typing import Any

import pyarrow as pa

from avacache import Client


def _print_err(msg: str) -> None:
    print(msg, file=sys.stderr)


def _json_default(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    raise TypeError(f"not json-serializable: {type(value).__name__}")


def _emit_rows(table: pa.Table, fmt: str, limit: int | None) -> None:
    if limit is not None:
        table = table.slice(0, limit)

    if fmt == "table":
        print(table.to_pandas().to_string(index=False) if _has_pandas() else table.to_string())
        return

    if fmt == "ndjson":
        for row in table.to_pylist():
            sys.stdout.write(json.dumps(row, default=_json_default))
            sys.stdout.write("\n")
        return

    if fmt == "json":
        json.dump(table.to_pylist(), sys.stdout, default=_json_default)
        sys.stdout.write("\n")
        return

    if fmt == "csv":
        rows = table.to_pylist()
        if not rows:
            return
        writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
        return

    raise ValueError(f"unknown format {fmt!r}")


def _has_pandas() -> bool:
    try:
        import pandas  # noqa: F401
        return True
    except ImportError:
        return False


def _default_format() -> str:
    return "table" if sys.stdout.isatty() else "ndjson"


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="avacache",
        description="Thin CLI over the avacache SDK. See .claude/skills/avacache/SKILL.md for the agent contract.",
    )
    p.add_argument("--base-url", help="Override archive base URL (or set AVACACHE_BASE_URL).")
    p.add_argument("--cache-dir", help="Override cache directory (or set AVACACHE_CACHE_DIR).")
    p.add_argument("--chain-id", type=int, default=None, help="Override chain id (default 43114).")
    p.add_argument("--offline", action="store_true", help="Refuse network; serve only from local cache.")
    p.add_argument("--no-decode-hex", action="store_true", help="Skip hex decoding; emit raw upstream strings.")

    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("manifest", help="Show manifest summary (or --json for the full document).")
    sp.add_argument("--json", action="store_true", help="Dump the full manifest JSON.")
    sp.add_argument("--force", action="store_true", help="Bypass the in-process manifest TTL.")

    sp = sub.add_parser("latest", help="Print latest_complete_date (empty string if none).")

    sp = sub.add_parser("dates", help="Print available dates for a kind, one per line.")
    sp.add_argument("--kind", choices=("blocks", "txs", "events"), default="txs")

    sp = sub.add_parser("url", help="Print the verified parquet URL for one (date, kind).")
    sp.add_argument("date")
    sp.add_argument("kind", choices=("blocks", "txs", "events"))

    sp = sub.add_parser("fetch", help="Download one (date, kind) parquet via the SDK (verifies md5+size).")
    sp.add_argument("date")
    sp.add_argument("kind", choices=("blocks", "txs", "events"))
    sp.add_argument("--out", help="Write parquet bytes to this path. Default: print the cache path.")

    sp = sub.add_parser("show", help="Load and print rows for one (date, kind).")
    sp.add_argument("date")
    sp.add_argument("kind", choices=("blocks", "txs", "events"))
    sp.add_argument("--limit", type=int, default=20, help="Row cap (default 20). Use 0 for all rows.")
    sp.add_argument("--format", choices=("table", "ndjson", "json", "csv"), default=None,
                    help="Output format. Default: table on a tty, ndjson otherwise.")

    sp = sub.add_parser("lookup", help="Resolve a function selector or event topic via the published lookup files.")
    sp.add_argument("kind", choices=("selector", "topic"))
    sp.add_argument("key", help="0x-prefixed 4-byte selector or 32-byte topic0 hash.")
    sp.add_argument("--json", action="store_true", help="Print the full match (signature + abi) as JSON.")

    return p


def _client_from_args(args: argparse.Namespace) -> Client:
    kwargs: dict[str, Any] = {}
    if args.base_url:
        kwargs["base_url"] = args.base_url
    if args.cache_dir:
        kwargs["cache_dir"] = args.cache_dir
    if args.chain_id is not None:
        kwargs["chain_id"] = args.chain_id
    if args.offline:
        kwargs["offline"] = True
    if args.no_decode_hex:
        kwargs["decode_hex"] = False
    return Client(**kwargs)


def _cmd_manifest(client: Client, args: argparse.Namespace) -> int:
    m = client.manifest(force=args.force)
    if args.json:
        json.dump(m.model_dump(), sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
        return 0

    print(f"chain_id:             {m.chain_id}")
    print(f"schema_version:       {m.schema_version}")
    print(f"generated_at:         {m.generated_at}")
    print(f"latest_complete_date: {m.latest_complete_date or '(none)'}")
    by_kind: dict[str, list] = {"blocks": [], "txs": [], "events": []}
    total_bytes = 0
    for f in m.files:
        by_kind.setdefault(f.kind, []).append(f)
        total_bytes += f.size
    print(f"files:                {len(m.files)} ({total_bytes / (1024**3):.2f} GiB)")
    for kind, entries in by_kind.items():
        if entries:
            dates = sorted(e.date for e in entries)
            print(f"  {kind:<7} {len(entries)} files  {dates[0]}..{dates[-1]}")
    if m.lookups:
        print(f"lookups:              {', '.join(sorted(m.lookups))}")
    return 0


def _cmd_latest(client: Client, _args: argparse.Namespace) -> int:
    latest = client.latest_complete_date()
    if latest is None:
        return 1
    print(latest.isoformat())
    return 0


def _cmd_dates(client: Client, args: argparse.Namespace) -> int:
    for d in client.available_dates(args.kind):
        print(d.isoformat())
    return 0


def _cmd_url(client: Client, args: argparse.Namespace) -> int:
    entry = client.manifest().entry(args.date, args.kind)
    if entry is None:
        _print_err(f"no {args.kind} parquet for {args.date} in manifest")
        return 2
    print(f"{client.base_url}/{entry.key}")
    return 0


def _cmd_fetch(client: Client, args: argparse.Namespace) -> int:
    entry = client.manifest().entry(args.date, args.kind)
    if entry is None:
        _print_err(f"no {args.kind} parquet for {args.date} in manifest")
        return 2
    cached = client._fetch_to_cache(entry.key, entry.md5, entry.size)
    if args.out:
        from shutil import copyfile
        copyfile(cached, args.out)
        print(args.out)
    else:
        print(cached)
    return 0


def _cmd_show(client: Client, args: argparse.Namespace) -> int:
    table = client.load_day(args.date, args.kind)
    limit = None if args.limit == 0 else args.limit
    fmt = args.format or _default_format()
    _emit_rows(table, fmt, limit)
    return 0


def _fetch_lookup_key(
    client: Client, lookup_name: str, key: str,
) -> list | None:
    """Stream the lookup file and return only the candidate list for `key`.

    The published `function_selectors.json` is hundreds of MB — buffering
    + ``json.loads`` would need multiple GB of memory for a one-key
    lookup. Same shape as the TS CLI's streaming scan: hash + size-verify
    on the fly, scan for ``"<key>":``, then bracket-walk just the
    matching value.

    Honors ``client.offline``: refuses network access so ``--offline``
    /  ``AVACACHE_OFFLINE=1`` keeps its contract.
    """
    if client.offline:
        raise RuntimeError(
            f"AVACACHE_OFFLINE=1 but lookup {lookup_name!r} would require a network fetch"
        )

    manifest = client.manifest()
    entry = manifest.lookups.get(lookup_name)
    if entry is None:
        raise FileNotFoundError(f"manifest does not publish lookup {lookup_name!r}")

    target = f'"{key.lower()}":'.encode()
    url = f"{client.base_url}/{entry.key}"
    hasher = hashlib.md5()
    total = 0
    overlap_bytes = 1 << 20  # 1 MiB sliding window — far larger than any one entry
    window = bytearray()
    captured: bytearray | None = None
    depth = 0
    in_string = False
    escaping = False

    def consume_for_capture(chunk: bytes) -> bool:
        """Append `chunk` to `captured`, return True when bracket depth hits 0."""
        nonlocal depth, in_string, escaping
        assert captured is not None
        for b in chunk:
            captured.append(b)
            if escaping:
                escaping = False
                continue
            if b == 0x5C and in_string:  # backslash inside a string
                escaping = True
                continue
            if b == 0x22:  # double quote
                in_string = not in_string
                continue
            if in_string:
                continue
            if b in (0x5B, 0x7B):  # '[' or '{'
                depth += 1
            elif b in (0x5D, 0x7D):  # ']' or '}'
                depth -= 1
                if depth == 0:
                    return True
        return False

    done = False  # latch: True once the matched value has been fully captured
    with client._http.stream("GET", url) as resp:
        resp.raise_for_status()
        # No chunk_size: yield chunks as the network delivers them. Forcing a
        # large chunk_size would buffer past the close of the matched value
        # and complicate the latch below.
        for chunk in resp.iter_bytes():
            total += len(chunk)
            hasher.update(chunk)
            if done:
                # Keep draining bytes for size + md5 verification, but never
                # append past the close of the matched value — otherwise the
                # captured fragment grows trailing JSON that breaks `loads`.
                continue
            if captured is not None:
                if consume_for_capture(chunk):
                    done = True
                continue
            window += chunk
            idx = window.find(target)
            if idx != -1:
                # Hand off everything after the target marker into the value scanner.
                captured = bytearray()
                if consume_for_capture(bytes(window[idx + len(target):])):
                    done = True
                window = bytearray()
            elif len(window) > overlap_bytes:
                # Keep only the trailing window so a key spanning two chunks still resolves.
                del window[: len(window) - overlap_bytes]

    if total != entry.size:
        raise ValueError(
            f"{lookup_name} size mismatch: got {total}, expected {entry.size}"
        )
    digest = hasher.hexdigest()
    if digest != entry.md5:
        raise ValueError(
            f"{lookup_name} md5 mismatch: got {digest}, expected {entry.md5}"
        )

    if captured is None:
        return None
    parsed = json.loads(bytes(captured))
    return parsed if isinstance(parsed, list) else [parsed]


def _cmd_lookup(client: Client, args: argparse.Namespace) -> int:
    name = "function_selectors" if args.kind == "selector" else "event_topics"
    candidates = _fetch_lookup_key(client, name, args.key)
    if candidates is None:
        _print_err(f"no entry for {args.key} in {name}")
        return 1
    # The archive maps each key to a list of candidates (selector / topic
    # collisions are real). Sort by observed `count` desc so the most
    # commonly-seen signature comes first.
    candidates = sorted(candidates, key=lambda c: c.get("count", 0), reverse=True)
    if args.json:
        json.dump(candidates, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        for c in candidates:
            sig = c.get("signature", "")
            count = c.get("count")
            print(f"{sig}\t{count}" if count is not None else sig)
    return 0


_DISPATCH = {
    "manifest": _cmd_manifest,
    "latest": _cmd_latest,
    "dates": _cmd_dates,
    "url": _cmd_url,
    "fetch": _cmd_fetch,
    "show": _cmd_show,
    "lookup": _cmd_lookup,
}


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    handler = _DISPATCH[args.cmd]
    with _client_from_args(args) as client:
        return handler(client, args)


if __name__ == "__main__":
    raise SystemExit(main())
