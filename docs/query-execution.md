# AtlasDB Query Execution Flow

## End-to-End Pipeline

1. Parse
   - Convert statement text into tokens and AST.
2. Bind
   - Resolve table and column names using catalog metadata.
3. Plan
   - Select execution path: table scan or index scan.
4. Execute
   - Run operators and return row/result status.
5. Persist
   - Route writes through pager and transaction/WAL path.

## Determinism Requirements

- Invalid statements must emit stable error codes.
- Duplicate-key handling policy must be consistent.
- Planner selection must be reproducible for identical metadata.

## Prepared Statements (Planned)

- Normalize SQL text into cache keys.
- Reuse parse/plan artifacts when schema epoch matches.
- Invalidate cache entries on DDL changes.
