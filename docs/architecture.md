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
   - Current: deterministic catalog snapshot load/save through pager metadata.
   - Planned: WAL append/checkpoint/recovery flow.

## Module Boundaries

- include/atlasdb: public API surface.
- src/core: engine and statement orchestration.
- src/parser: lexer/parser and syntax diagnostics.
- src/catalog: table/index metadata management.
- src/storage: pager and on-disk representation.
- src/btree: index node logic and split operations.
- src/txn: transaction manager and WAL.

Current implemented execution path:

- parser -> in-memory catalog execution for CREATE/INSERT/SELECT/UPDATE/DELETE,
- planning layer now includes a deterministic RulePlanner primitive for table-scan vs primary-key index-lookup path selection from statement + table metadata,
- optional persistence mode (`DatabaseEngine(path)`) wires mutating statements to pager-backed catalog snapshots,
- startup reloads latest snapshot using page-0 metadata (`catalog_root_page`, `schema_epoch`).
- storage primitives now include typed row codec plus slotted row-page append/read/count layout utilities.
- storage also includes a pager-backed table-store directory for managing row-data pages and row locations.
- persistence-mode SELECT reads are decoded from table-store scans while write execution remains catalog-first.
- persistence-mode CREATE now initializes table-store pages for the newly created table with rebuild fallback for consistency.
- persistence-mode INSERT now appends into table-store pages after catalog+snapshot success, with rebuild fallback for consistency.
- persistence-mode UPDATE now rebuilds only the affected table-store from catalog rows, with fallback to full table-store rebuild.
- persistence-mode DELETE now rebuilds only the affected table-store from catalog rows, with fallback to full table-store rebuild.
- index layer now includes a deterministic B+ tree leaf-node primitive for ordered key/row-location entries and next-leaf links.
- index layer now also includes a deterministic B+ tree internal-node primitive for ordered separators and child-page routing.
- index layer now includes a deterministic pager-backed linked-leaf cursor primitive for seek and next traversal.
- index layer now includes deterministic leaf split logic that produces promoted separator metadata for parent update paths.
- index layer now includes deterministic internal separator insertion and split-root initialization helpers for parent update paths.
- index layer now includes deterministic internal split logic that emits promoted separator metadata for upward propagation.
- index layer now includes deterministic pager-backed BtreeIndex orchestration for insert/find, recursive split propagation, and root growth.
- runtime now invokes RulePlanner before statement execution dispatch while execution paths still intentionally use existing catalog/table-store handlers until index-backed dispatch slices land.

## Invariants

- Deterministic error messages and codes for identical invalid inputs.
- Stable page layout once file format version is released.
- Snapshot metadata updates are monotonic (`schema_epoch` increments per successful mutation in persistence mode).
- B+ tree ordering invariants maintained after every mutation.
- WAL replay is idempotent for committed transactions.
