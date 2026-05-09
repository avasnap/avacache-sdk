import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    environment: 'node',
    // Use the forks pool (child_process) so we can pass --expose-gc.
    // Worker threads reject --expose-gc as an invalid execArgv. The
    // bounded-memory regression test in client.test.ts depends on
    // deterministic GC to prove that consumed prefetch buffers are
    // reclaimed, not retained by the streaming iterators.
    pool: 'forks',
    poolOptions: {
      forks: { execArgv: ['--expose-gc'] },
    },
  },
});
