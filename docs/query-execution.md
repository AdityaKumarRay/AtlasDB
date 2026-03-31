# AtlasDB Query Execution Flow

## End-to-End Pipeline

1. Parse
   - Convert statement text into tokens and AST.
2. Bind
   - Resolve table and column names using catalog metadata.
3. Plan
   - Select execution path: table scan or index scan.
4. Execute
   - Run statement handlers and return deterministic runtime status.
5. Persist
   - In persistence mode, serialize and flush a catalog snapshot through pager metadata updates.
   - Planned: route writes through row/page operators plus WAL path.

## Current Runtime Scope

- CREATE TABLE, INSERT, SELECT \* FROM, UPDATE, and DELETE are executed against an in-memory catalog.
- The same statement set supports optional startup/load persistence when the engine is opened with a file path.
- In persistence mode, SELECT now decodes rows from table-store scans (directory-managed row pages).
- In persistence mode, CREATE now initializes table-store pages for the newly created table after catalog+snapshot success.
- In persistence mode, INSERT now appends row payloads into table-store pages after catalog+snapshot success.
- In persistence mode, UPDATE now rebuilds only the updated table-store from catalog rows after catalog+snapshot success.
- In persistence mode, DELETE now rebuilds only the deleted table-store from catalog rows after catalog+snapshot success.
- Table and column identifiers are resolved case-insensitively.
- Runtime checks currently enforced:
  - duplicate table names,
  - unknown table on insert/select,
  - unknown columns in UPDATE/DELETE,
  - UPDATE/DELETE predicate must target PRIMARY KEY,
  - value count mismatch,
  - literal type mismatch,
  - duplicate PRIMARY KEY values.

Runtime errors use deterministic `E2xxx` codes.

Persistence startup/write failures use deterministic `E4xxx` and pager (`E31xx`) codes.

SELECT output is deterministic and currently emitted as an ordered row list in insertion order.
UPDATE and DELETE currently affect at most one row because predicates are restricted to PRIMARY KEY equality.

Persistence note:

- low-level pager and page/header codecs are implemented,
- catalog snapshot persistence is implemented for successful mutating statements,
- table-store storage primitives now support append/read/scan across directory-managed row pages,
- CREATE uses direct table-store initialization for the new table with deterministic rebuild fallback if initialization checks fail,
- UPDATE uses table-scoped table-store rebuild with deterministic full-rebuild fallback if table-scoped rebuild checks fail,
- DELETE uses table-scoped table-store rebuild with deterministic full-rebuild fallback if table-scoped rebuild checks fail,
- INSERT uses direct table-store append with deterministic rebuild fallback if append-path consistency checks fail,
- index-page primitives now include deterministic B+ tree leaf/internal node layouts and validations,
- table/index page-oriented physical operators are still planned.

## Determinism Requirements

- Invalid statements must emit stable error codes.
- Duplicate-key handling policy must be consistent.
- Planner selection must be reproducible for identical metadata.

## Prepared Statements (Planned)

- Normalize SQL text into cache keys.
- Reuse parse/plan artifacts when schema epoch matches.
- Invalidate cache entries on DDL changes.
