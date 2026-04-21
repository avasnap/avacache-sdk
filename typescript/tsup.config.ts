import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts', 'src/duckdb.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  splitting: false,
  sourcemap: true,
  clean: true,
  target: 'es2022',
  platform: 'neutral',
});
