# AtlasDB Storage Format

## Principles

- Fixed-size pages (target 4096 bytes).
- Versioned file header for compatibility checks.
- Explicit encoding for typed column values.
- Deterministic serialization and deserialization.

## Planned File Layout

- Page 0: database header
  - magic bytes
  - format version
  - page size
  - root page references
  - checkpoint metadata
- Catalog pages: table/index definitions
- Data pages: table rows in slotted-page layout
- Index pages: B+ tree leaf/internal node layouts

## Implemented Catalog Snapshot Layout (Current)

AtlasDB now persists catalog state as deterministic snapshots.

- Page 0 header stores the latest `catalog_root_page` and `schema_epoch`.
- Snapshot pages are currently written as a contiguous page run starting at `catalog_root_page`.
- Snapshot binary header (first 16 bytes) stores:
  - magic bytes (`ATLSNAP\0`),
  - snapshot format version (`1`),
  - payload size in bytes.
- Payload bytes are the serialized in-memory catalog (tables, typed columns, rows, and primary-key metadata).

Operational behavior:

- Opening a database validates page-0 metadata and loads the latest snapshot when `catalog_root_page != 0`.
- Each successful CREATE/INSERT/UPDATE/DELETE in persistence mode writes a new full catalog snapshot and advances `schema_epoch`.
- Older snapshots remain append-only for now; snapshot GC/compaction is a later task.

## Implemented Header Page (Current)

The current implementation serializes and validates the database file header in page 0.

Header fields currently encoded:

- magic bytes (`ATLASDB\0`)
- format version (`kFileFormatVersion`)
- page size (`kPageSize`)
- page count
- catalog root page id
- schema epoch
- checkpoint LSN

Validation currently enforced during deserialization:

- magic must match expected bytes,
- format version must be supported,
- page size must match build-time page size,
- page count must be non-zero,
- catalog root page must be within page-count bounds when non-zero.

## Implemented Pager Foundations (Current)

The current pager implementation provides deterministic file and page I/O primitives.

Implemented behavior:

- open existing database files and validate page-0 header,
- create new database files with bootstrapped page-0 header,
- read and write fixed-size pages by page id,
- allocate new zeroed pages,
- persist page_count updates through header rewrites.

Current operational constraints:

- page ids must be less than header.page_count for read/write,
- newly allocated pages are appended at id = previous page_count,
- SQL execution remains memory-first (full table pages are not yet implemented),
- persistence currently captures catalog+row state via full snapshot rewrites.

Deterministic snapshot-related error codes currently used:

- `E4001` invalid/truncated snapshot header or magic,
- `E4002` unsupported snapshot version,
- `E4003` snapshot payload too large,
- `E4004` snapshot page-id overflow,
- `E3108` invalid pager catalog-metadata update request.

## Implemented Row Codec Foundations (Current)

AtlasDB now includes a deterministic row codec for typed value payloads.

- Row payload starts with a 16-bit column count.
- Each value is prefixed with a 1-byte tag:
  - `1` for INTEGER (stored as 8-byte little-endian signed payload),
  - `2` for TEXT (stored as 4-byte length + raw bytes).
- Decode validates schema shape and type tags against expected column types.

Deterministic row-codec error codes currently used:

- `E3200` output pointer is null,
- `E3201` column/value count mismatch,
- `E3202` value type mismatch for schema column,
- `E3203` text literal exceeds codec size limit,
- `E3204` truncated payload while decoding,
- `E3205` schema/tag mismatch while decoding,
- `E3206` trailing bytes after decoding.

## Implemented Slotted Row Page Foundations (Current)

AtlasDB now includes a deterministic slotted row-page primitive.

- Page header (8 bytes) stores:
  - format version,
  - row count,
  - free-start pointer (slot directory end),
  - free-end pointer (row payload start).
- Slot directory entries are 4 bytes each:
  - 16-bit row offset,
  - 16-bit row size.
- Appends are stable by slot order and grow the slot directory upward while row payload grows downward.

Deterministic row-page error codes currently used:

- `E3300` null pointer for page/output arguments,
- `E3301` invalid row-page header/layout invariants,
- `E3302` row/slot limit overflow,
- `E3303` insufficient free space for new row payload + slot entry,
- `E3304` slot index out of range,
- `E3305` invalid slot payload range during read,
- `E3306` empty row payload append.

## Implemented Table Store Foundations (Current)

AtlasDB now includes a pager-backed table-store primitive for row-page management.

- Table root page stores a directory header with:
  - magic (`ATLTDIR\0`),
  - directory format version,
  - data-page count,
  - total row count.
- Directory entries are 32-bit row-data page ids.
- Appends target the last data page and allocate a new row page when the tail page is full.
- Row reads validate that the requested row page belongs to the table directory.
- Row scans traverse data pages in directory order and validate scanned row count against directory metadata.
- Persistence-mode CREATE initializes a fresh table-store root+first data page for each newly created table.
- Persistence-mode INSERT currently routes row payload writes through table-store append semantics.
- Persistence-mode UPDATE currently refreshes only the affected table-store by rebuilding that table's directory/data pages from catalog rows.
- Persistence-mode DELETE currently refreshes only the affected table-store by rebuilding that table's directory/data pages from catalog rows.
- Persistence-mode SELECT currently consumes decoded table-store scan rows.

Deterministic table-store error codes currently used:

- `E3400` null pointer argument,
- `E3401` pager not open,
- `E3402` invalid directory layout/version,
- `E3403` pager read/write/allocate failure wrapper,
- `E3404` row-count integrity mismatch during scan,
- `E3405` row location page does not belong to table,
- `E3406` directory entry capacity exhausted.

## WAL Overview

- Append-only log records with checksums.
- Transaction begin/commit markers.
- Checkpoint records for truncation boundaries.
- Startup recovery replays committed operations.

## Compatibility

Any format change must:

- bump file format version when incompatible,
- document migration strategy,
- include persistence regression tests.
