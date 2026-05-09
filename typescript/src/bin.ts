/**
 * Binary entry point for the `avacache` CLI.
 *
 * Kept tiny on purpose: when this module loads, we know we're being run
 * as a binary (via `node dist/bin.js`, the npm `.bin/avacache` symlink,
 * or `npx avacache`), so `main()` runs unconditionally. The shared logic
 * lives in `cli.ts`, which exports `main()` without auto-running so the
 * test suite can import it safely.
 *
 * Detecting "am I the entry point?" via `import.meta.url` vs
 * `process.argv[1]` is unreliable across npm bin symlinks on POSIX —
 * Node resolves the URL through realpath while argv[1] keeps the
 * symlink, so an `endsWith` check silently no-ops the CLI. Splitting
 * the file removes the guess.
 */

import { main } from './cli.js';

main().then((code) => process.exit(code));
