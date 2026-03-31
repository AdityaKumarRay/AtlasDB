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
