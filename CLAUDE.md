# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

This repo ships **two sibling SDKs** that target the same public dataset — the Avalanche C-Chain parquet archive at `https://parquet.avacache.com`. They are intentionally independent packages published under the same name on different registries:

- `python/` — PyPI package `avacache` (hatchling, pyarrow)
- `typescript/` — npm package `avacache` (tsup, hyparquet, isomorphic Node/browser)

There is no top-level workspace, Makefile, or shared tooling. Run commands from inside the package directory you are working on. Keep parity in the **client surface** (`manifest()`, `load_day` / `loadDay`, `load_range` / `loadRange`, hex decoding opt-out) when changing either — this is the contract users rely on, not an accident.

## Common commands

### Python (`cd python/`)

```bash
pip install -e '.[dev]'            # editable install with pytest + respx + ruff
pytest                             # mocked tests (respx) — no network
pytest -m integration              # hits real parquet.avacache.com
pytest tests/test_client.py::test_load_day  # single test
ruff check src tests
```

### TypeScript (`cd typescript/`)

```bash
npm install
npm run build                      # tsup → dist/ (esm + cjs + dts) for two entries: index, duckdb
npm test                           # vitest run
npm run test:watch
npm run typecheck                  # tsc --noEmit
npx vitest run tests/client.test.ts -t 'loads a day'   # single test by name
```

The TypeScript `duckdb.ts` entry depends on `@duckdb/duckdb-wasm` as an **optional peer dep** — don't import it from `src/index.ts` or it leaks into the core bundle.

## Architecture

Both SDKs implement the same three-layer pipeline. When editing either, mirror the change in the other unless there is a deliberate reason not to.

1. **Manifest.** `manifest.json` at the archive root is the single source of truth: chain id, `latest_complete_date`, per-day `files[]` with `{date, kind, key, size, md5, schema_version}`, and `columns{}` / `lookups{}`. The client fetches it once per 5-minute TTL, caches on disk (Python) / in memory (TS). All reads go through it — never construct a parquet URL without consulting the manifest for the matching `md5` + `size`.
2. **Fetch & verify.** `_fetch_to_cache` (py) / `fetchBytes` (ts) downloads `{baseUrl}/{entry.key}`, streams to a tmp file, MD5-checks against the manifest entry, then atomically renames into place. In browsers WebCrypto has no MD5, so TS falls back to size-only verification — this is intentional, document it if you touch that path.
3. **Decode.** Hex-encoded numeric columns are converted at load time:
   - wei-scale (uint256) → `DECIMAL(38,0)` (Python) / `bigint` (TS)
   - small ints (`log_index`) → `int64` / `number`
   The column lists live in `hex.py` / `hex.ts` keyed by `kind` (`blocks` | `txs` | `events`). When the upstream schema adds a new hex column, update **both** lists. Callers can opt out with `decode_hex=False` / `decodeHex: false`.

### Cache layout

Both clients cache under `~/.cache/avacache/v1/<chain_id>/` by default; override via `AVACACHE_CACHE_DIR`. Python files are keyed `daily/<date>.<kind>.<md5[:16]>.parquet` so stale files are never read — if the manifest md5 changes, the cache hash in the filename also changes and a re-download is forced. The TS Node cache uses the full `entry.key|entry.md5` as the filename after `/`→`__` escaping for the same reason. The browser cache uses IndexedDB (`avacache-v1-<chain_id>`).

### DuckDB helpers

`open_day` / `open_range` (Python) and `openDay` (TS) register `blocks`, `txs`, `events` as views so callers can run SQL without learning the layout. Both are optional — Python gates the import behind the `[duckdb]` extra and TS gates on the `@duckdb/duckdb-wasm` peer dep. Don't promote these to required dependencies.

### Range loading

`load_range` loads the full date span into memory as one concatenated table. For multi-month ranges of `txs` or `events` that can OOM — Python exposes `iter_range()` which yields one decoded table at a time while prefetching the next `concurrency` files. The TS client currently has only `loadRange` (serial). If a user asks for a streaming TS equivalent, add `iterRange` rather than retrofitting `loadRange`.

## Testing notes

Python tests use `respx` to mock the archive over `httpx` — the `mock_archive` fixture in `tests/conftest.py` builds real parquet bytes in memory (so decoding is genuinely exercised) and serves them plus a matching manifest at `https://cache.example.test`. Tests marked `@pytest.mark.integration` hit the real R2 origin and are off by default.

TypeScript tests (`tests/client.test.ts`) use MSW to mock the archive identically. Keep the two mock shapes in sync — if you add a new kind or column, update both fixtures.

## Environment variables

- `AVACACHE_BASE_URL` — override archive host (both SDKs)
- `AVACACHE_CACHE_DIR` — override local cache path (both SDKs)
- `AVACACHE_OFFLINE=1` — Python only; refuse network, serve only from local cache. If the TS client ever grows an offline switch, use the same name.
