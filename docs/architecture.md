# AtlasDB Architecture

## Layered Design

AtlasDB uses a layered architecture with explicit boundaries.

1. Interface Layer
   - REPL and command ingestion.
   - Statement lifecycle and diagnostics.
2. Language Layer
   - Lexer and parser to create AST nodes.
   - Binder to resolve names against schema metadata.
3. Execution Layer
   - Planner selects physical operators.
   - Executor runs operators against storage and indexes.
4. Storage Layer
   - Pager and page cache abstractions.
   - File format and row codecs.
5. Index Layer
   - B+ tree implementation and cursor traversal.
6. Durability Layer
   - WAL append/checkpoint/recovery flow.

## Module Boundaries

- include/atlasdb: public API surface.
- src/core: engine and statement orchestration.
- src/parser: lexer/parser and syntax diagnostics.
- src/catalog: table/index metadata management.
- src/storage: pager and on-disk representation.
- src/btree: index node logic and split operations.
- src/txn: transaction manager and WAL.

## Invariants

- Deterministic error messages and codes for identical invalid inputs.
- Stable page layout once file format version is released.
- B+ tree ordering invariants maintained after every mutation.
- WAL replay is idempotent for committed transactions.
