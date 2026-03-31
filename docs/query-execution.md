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

- CREATE TABLE, INSERT, and SELECT \* FROM are executed against an in-memory catalog.
- Table and column identifiers are resolved case-insensitively.
- Runtime checks currently enforced:
  - duplicate table names,
  - unknown table on insert/select,
  - value count mismatch,
  - literal type mismatch,
  - duplicate PRIMARY KEY values.

Runtime errors use deterministic `E2xxx` codes.

SELECT output is deterministic and currently emitted as an ordered row list in insertion order.

## Determinism Requirements

- Invalid statements must emit stable error codes.
- Duplicate-key handling policy must be consistent.
- Planner selection must be reproducible for identical metadata.

## Prepared Statements (Planned)

- Normalize SQL text into cache keys.
- Reuse parse/plan artifacts when schema epoch matches.
- Invalidate cache entries on DDL changes.
