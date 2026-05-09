/**
 * Thin wrappers over hyparquet. We always have the parquet bytes in hand
 * (fetched + cached by `client.ts`), so we skip hyparquet's URL/range
 * machinery and construct an in-memory AsyncBuffer directly.
 */

import {
  parquetMetadataAsync,
  parquetReadObjects,
  type AsyncBuffer,
  type FileMetaData,
} from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

function bytesToAsyncBuffer(bytes: Uint8Array): AsyncBuffer {
  const buffer = bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  ) as ArrayBuffer;
  return {
    byteLength: buffer.byteLength,
    slice: (start: number, end?: number): Promise<ArrayBuffer> =>
      Promise.resolve(buffer.slice(start, end ?? buffer.byteLength)),
  };
}

export async function readRowsFromBytes(
  bytes: Uint8Array,
): Promise<Record<string, unknown>[]> {
  const file = bytesToAsyncBuffer(bytes);
  return (await parquetReadObjects({ file, compressors })) as Record<
    string,
    unknown
  >[];
}

/**
 * Walk a parquet file one row group at a time, yielding each row in turn.
 *
 * The hyparquet `parquetReadObjects` API materializes every row before it
 * returns; calling it once per row group gives us bounded peak memory (one
 * group's worth of rows at a time) without losing column projection.
 *
 * Pass `columns` to project only the fields you need — hyparquet skips the
 * column-chunk decode for everything else, which is the single biggest
 * memory and CPU lever for wide tables like `events`.
 */
export async function* iterRowsFromBytes(
  bytes: Uint8Array,
  opts: { columns?: readonly string[] } = {},
): AsyncGenerator<Record<string, unknown>, void, void> {
  const file = bytesToAsyncBuffer(bytes);
  const metadata: FileMetaData = await parquetMetadataAsync(file);
  const columns = opts.columns ? [...opts.columns] : undefined;

  let rowStart = 0;
  for (const rg of metadata.row_groups) {
    const groupRows = Number(rg.num_rows);
    const rowEnd = rowStart + groupRows;
    if (groupRows > 0) {
      const rows = (await parquetReadObjects({
        file,
        metadata,
        columns,
        rowStart,
        rowEnd,
        compressors,
      })) as Record<string, unknown>[];
      for (const row of rows) yield row;
    }
    rowStart = rowEnd;
  }
}
