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
