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
   - Route writes through pager and transaction/WAL path.

## Current Runtime Scope

- CREATE TABLE, INSERT, SELECT \* FROM, UPDATE, and DELETE are executed against an in-memory catalog.
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

SELECT output is deterministic and currently emitted as an ordered row list in insertion order.
UPDATE and DELETE currently affect at most one row because predicates are restricted to PRIMARY KEY equality.

Persistence note:

- low-level pager and page/header codecs are implemented,
- SQL CREATE/INSERT/SELECT/UPDATE/DELETE flows are still memory-backed in the current phase.

## Determinism Requirements

- Invalid statements must emit stable error codes.
- Duplicate-key handling policy must be consistent.
- Planner selection must be reproducible for identical metadata.

## Prepared Statements (Planned)

- Normalize SQL text into cache keys.
- Reuse parse/plan artifacts when schema epoch matches.
- Invalidate cache entries on DDL changes.
