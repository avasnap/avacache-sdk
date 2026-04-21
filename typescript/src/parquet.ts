/**
 * Thin wrappers over hyparquet. We always have the parquet bytes in hand
 * (fetched + cached by `client.ts`), so we skip hyparquet's URL/range
 * machinery and construct an in-memory AsyncBuffer directly.
 */

import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

export async function readRowsFromBytes(
  bytes: Uint8Array,
): Promise<Record<string, unknown>[]> {
  const buffer = bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  ) as ArrayBuffer;

  const file = {
    byteLength: buffer.byteLength,
    slice: (start: number, end?: number): Promise<ArrayBuffer> =>
      Promise.resolve(buffer.slice(start, end ?? buffer.byteLength)),
  };

  return (await parquetReadObjects({ file, compressors })) as Record<
    string,
    unknown
  >[];
}
