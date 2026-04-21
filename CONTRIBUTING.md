# Contributing

This repository contains two sibling SDKs for the same public dataset: the
Avalanche C-Chain parquet archive at `https://parquet.avacache.com`.

- `python/` publishes the PyPI package `avacache`
- `typescript/` publishes the npm package `avacache`

They are independent packages, not a monorepo workspace. Run commands from the
package directory you are changing.

## Repository Layout

- [README.md](README.md): repo overview and shared usage guidance
- [docs/archive-contract.md](docs/archive-contract.md): public archive contract
- `python/`: Python SDK source, tests, and package metadata
- `typescript/`: TypeScript SDK source, tests, and package metadata
- [CLAUDE.md](CLAUDE.md): maintainer notes that informed this guide

Within each package:

- `src/`: implementation
- `tests/`: package-specific test suite
- `README.md`: public package documentation

## Before You Start

- Keep changes scoped to the package or docs you intend to touch.
- Assume the worktree may already contain unrelated edits. Do not revert or
  rewrite them unless you coordinated that change explicitly.
- If you change shared behavior, update the other SDK unless there is a clear,
  documented reason not to.
- Do not guess archive URLs or file availability. The manifest is the source of
  truth for dates, object keys, sizes, MD5s, schema version, and lookup files.

## Python Workflow

Work in `python/`.

Install for development:

```bash
cd python
pip install -e '.[dev]'
```

Common commands:

```bash
pytest
pytest -m integration
pytest tests/test_client.py::test_load_day
ruff check src tests
```

Notes:

- The Python package uses `hatchling`.
- Tests normally use `respx` and mocked parquet bytes, so they run without the
  public archive.
- Integration tests hit the real archive and are marked with
  `@pytest.mark.integration`.
- Optional extras such as `duckdb`, `pandas`, `polars`, and `notebook` should
  stay optional unless there is a strong reason to change packaging.

## TypeScript Workflow

Work in `typescript/`.

Install dependencies:

```bash
cd typescript
npm install
```

Common commands:

```bash
npm run build
npm test
npm run test:watch
npm run typecheck
npx vitest run tests/client.test.ts -t 'loads a day'
```

Notes:

- The TypeScript package targets Node 18+ and modern browsers.
- Core functionality must remain usable without DuckDB.
- `@duckdb/duckdb-wasm` is an optional peer dependency and should stay out of
  the main entrypoint unless the package design intentionally changes.
- Tests use MSW to mock the archive.

## Cross-SDK Parity

The two SDKs are intentionally separate implementations of the same archive
contract. Keep parity in the behaviors users rely on:

- manifest loading and refresh behavior
- day and range loading
- missing-date semantics for ranges
- hex decoding defaults and opt-out
- manifest-first URL resolution

Method names differ by language style, but the client surface should stay
conceptually aligned:

- Python: `manifest()`, `load_day()`, `load_range()`
- TypeScript: `manifest()`, `loadDay()`, `loadRange()`

If you add support for a new archive field, kind, or decode rule in one SDK,
update the matching implementation and tests in the other SDK as part of the
same contribution whenever possible.

## Testing Guidance

Run the tests that match your scope before you open a PR.

- Python-only change: run the relevant Python tests and `ruff check`
- TypeScript-only change: run the relevant TypeScript tests, `build`, and
  `typecheck`
- Shared behavior or docs that describe behavior: verify both packages still
  match the documented contract

When changing mocked archive data:

- Keep the Python and TypeScript test fixtures aligned
- Update both suites if a schema field, decode rule, or manifest shape changes

When touching integration behavior:

- Prefer mocked coverage first
- Run real-network integration tests only when the change genuinely depends on
  the live archive

## Documentation Expectations

This repo is public-facing. Documentation changes are part of the work, not an
afterthought.

Update docs when you change:

- the public client surface
- constructor options or environment variables
- cache or integrity behavior
- manifest semantics
- schema, lookup, or decode behavior
- DuckDB helper behavior

At minimum, update the package README for the affected SDK. If the change alters
the shared archive contract, also update [docs/archive-contract.md](docs/archive-contract.md)
and the root [README.md](README.md) when needed.

Write docs for humans:

- explain the workflow, not just the method name
- call out runtime-specific behavior when it matters
- document sharp edges such as offline mode, range memory cost, or decode
  differences

## Safe Contribution Rules

- Do not make DuckDB a required dependency in either SDK by accident.
- Do not bypass manifest validation by constructing parquet URLs directly in new
  code paths.
- Do not silently change cache keying or integrity checks without updating tests
  and docs.
- Do not introduce a Python-only or TypeScript-only contract change unless it is
  clearly intentional and documented.
- Do not remove or overwrite adjacent changes from other contributors just to
  make your patch cleaner.

## Submitting Changes

Before submitting:

1. Keep the diff focused.
2. Run the relevant package commands.
3. Update tests for behavioral changes.
4. Update docs for public-facing changes.
5. Note any deliberate parity exception in your PR description.

If a change is intentionally package-specific, explain why. The default
expectation in this repo is shared behavior across both SDKs.
