# AtlasDB

AtlasDB is a production-oriented embedded database engine written in modern C++.

The project is inspired by incremental database-kernel tutorials, but its architecture and engineering process target portfolio-quality systems work: modular subsystems, deterministic behavior, durable storage primitives, and continuous testing.

## Current Status

Phase-wise progress:

- [x] Phase 0: Project foundation, CI matrix, and repo hygiene.
- [x] Phase 1: Deterministic parser + diagnostics for CREATE/INSERT/SELECT/UPDATE/DELETE.
- [x] Phase 2: In-memory catalog execution for CRUD with deterministic runtime checks.
- [x] Phase 3: Pager-backed persistence foundations and page-native table storage primitives.
- [-] Phase 4: B+ tree primary index + cursor abstraction.
- [ ] Phase 5: Secondary indexes, planner, prepared statements.
- [ ] Phase 6: Transactions, WAL, checkpoint, recovery.
- [ ] Phase 7: Hardening, benchmarks, and release-readiness polish.

Current capabilities in place:

- C++20 + CMake project layout.
- Strict compiler warning policy.
- GoogleTest test harness.
- REPL plus deterministic parser and in-memory execution for CREATE TABLE, INSERT, SELECT, UPDATE, and DELETE.
- Storage foundation includes page/header codecs plus pager file I/O and page allocation primitives.
- Typed row codec foundation for INTEGER/TEXT literal serialization and validation.
- Slotted row-page foundation for deterministic row append/read within fixed-size pages.
- Pager-backed table-store primitive for directory-managed row pages, row-location reads, and ordered scans.
- Persistence-mode SELECT path decodes rows from table-store scans, while mutating statements still use catalog-first execution.
- Persistence-mode CREATE now initializes a table-store root for only the newly created table after catalog+snapshot success, with deterministic fallback rebuild on initialization failures.
- Persistence-mode INSERT now appends directly to table-store pages after catalog+snapshot success, with deterministic fallback rebuild on append-path failures.
- Persistence-mode UPDATE now rebuilds only the updated table's table-store pages after catalog+snapshot success, with deterministic fallback to full rebuild on table-scoped rebuild failures.
- Persistence-mode DELETE now rebuilds only the deleted table's table-store pages after catalog+snapshot success, with deterministic fallback to full rebuild on table-scoped rebuild failures.
- Phase 4 kickoff: deterministic B+ tree leaf-node page primitive with append/read/search/next-leaf operations.
- Phase 4 progress: deterministic B+ tree internal-node page primitive with ordered separator keys and child-page routing.
- Phase 4 progress: deterministic pager-backed linked-leaf cursor primitive with seek/next traversal.
- Phase 4 progress: deterministic leaf split primitive that emits separator-promotion metadata and preserves linked-leaf traversal order.
- Phase 4 progress: deterministic internal separator insertion and root-from-split initialization helpers for split-propagation scaffolding.
- Phase 4 progress: deterministic internal split primitive that emits promoted separator metadata for parent split propagation.
- Phase 4 progress: deterministic pager-backed B+ tree index primitive for insert/find, recursive split propagation, and root growth.
- Phase 4 hardening: deterministic seeded random-insert stress test now validates subtree key-range invariants and linked-leaf traversal consistency under heavy split workloads.
- Phase 4 hardening: deterministic reopen-and-continue-insert stress coverage now validates multi-level index invariants and linked-leaf consistency across pager reopen boundaries.
- Phase 4 integration coverage: primary-index tests now validate ordered traversal and point key lookup by joining B+ tree entries with table-store row payload decode paths.
- Optional pager-backed catalog snapshot persistence for CREATE/INSERT/UPDATE/DELETE when opening the engine with a database file path.
- GitHub Actions CI matrix for Windows and Linux (Debug and Release).

## Project Goals

- Build a compact but realistic embedded database kernel.
- Demonstrate storage internals, indexing, and recovery fundamentals.
- Keep code clean, tested, and easy to reason about.

## Architecture Overview

AtlasDB is organized into layered modules.

- Frontend: REPL, parser, and deterministic diagnostics.
- Catalog/Schema: table definitions, typed columns, index metadata.
- Planner/Executor: plan selection and statement execution.
- Storage: pager, page cache, row serialization, file format versioning.
- Access Methods: B+ tree indexes and cursor traversal.
- Durability: WAL, checkpoints, and startup recovery.

Detailed notes live under docs:

- docs/architecture.md
- docs/storage-format.md
- docs/query-execution.md
- docs/testing.md

## Storage Format (Planned v1)

The database file will use fixed-size pages with a versioned file header.

- File header page stores format version and global metadata.
- Catalog pages store schema metadata.
- Data and index pages use B+ tree node layouts.
- WAL records are checksummed and replayed on startup.

Current implemented foundation:

- Page primitive with a fixed 4096-byte page size.
- Header page codec with magic validation.
- Pager file open/create behavior with header bootstrap.
- Page read/write by page id.
- Deterministic page allocation that updates persisted page_count in page-0 header.
- Catalog snapshot persistence metadata via page-0 `catalog_root_page` and `schema_epoch` updates.
- Typed row codec for deterministic row-value encode/decode against schema columns.
- Slotted row-page layout primitives for row count, append, and slot-based read.
- Table-store layout for root directory pages that track row-data pages across pager reopen.
- Table-store scan primitive for ordered row traversal across directory-managed data pages.
- Deterministic header validation error codes:
  - `E3001` invalid file magic,
  - `E3002` unsupported file format version,
  - `E3003` unsupported page size,
  - `E3004` invalid page count,
  - `E3005` catalog root page out of range.
- Deterministic pager validation/error codes:
  - `E3101` pager/file open failures,
  - `E3102` page read/seek failures,
  - `E3103` declared page_count larger than on-disk file size,
  - `E3104` page read out of range,
  - `E3106` page write out of range,
  - `E3107` page write/flush failures,
  - `E3108` invalid catalog metadata update request,
  - `E3110` page id space exhaustion.

Catalog snapshot startup/load errors:

- `E4001` invalid or truncated catalog snapshot header/magic,
- `E4002` unsupported catalog snapshot version,
- `E4003` snapshot payload exceeds supported size,
- `E4004` snapshot page id overflow.

Row codec errors:

- `E3200` output pointer is null,
- `E3201` column/value shape mismatch,
- `E3202` type mismatch against schema column,
- `E3203` text literal exceeds codec size limit,
- `E3204` truncated row payload,
- `E3205` schema/tag mismatch while decoding,
- `E3206` trailing bytes in decoded row payload.

Row page errors:

- `E3300` null page/output pointer,
- `E3301` invalid row-page layout/version,
- `E3302` row payload or slot space limit exceeded,
- `E3303` insufficient free space on page,
- `E3304` slot index out of range,
- `E3305` corrupt slot payload range,
- `E3306` empty row payload append.

Table store errors:

- `E3400` null pointer for required output/argument,
- `E3401` table-store pager not open,
- `E3402` invalid table-directory page layout/version,
- `E3403` pager operation failure while loading/writing table-store pages,
- `E3404` directory row-count integrity mismatch during scan,
- `E3405` requested row page is not part of table directory,
- `E3406` table-directory entry capacity reached.

B+ tree leaf-node errors:

- `E5100` null pointer for required output/argument,
- `E5101` invalid leaf-node layout/magic/version/order,
- `E5102` entry index out of range,
- `E5103` leaf node is full,
- `E5104` appended key is not strictly increasing,
- `E5105` key not found in leaf node.

B+ tree internal-node errors:

- `E5200` null pointer for required output/argument,
- `E5201` invalid internal-node layout/magic/version/order,
- `E5202` invalid child page id,
- `E5203` internal node is full,
- `E5204` appended separator key is not strictly increasing,
- `E5205` separator entry index out of range,
- `E5206` root split metadata is invalid (for example duplicate child page ids),
- `E5207` internal split precondition/layout requirement failed.

B+ tree cursor errors:

- `E5300` null pointer for required output/argument,
- `E5301` cursor pager is unavailable or not open,
- `E5302` starting leaf page id is invalid,
- `E5303` pager read failed during cursor traversal,
- `E5304` cursor is not positioned on a valid entry,
- `E5305` leaf-chain traversal integrity failure.

B+ tree index errors:

- `E5400` null pointer for required output/argument,
- `E5401` index pager is unavailable/not open,
- `E5402` unknown root/tree node type,
- `E5403` index root page id is invalid/uninitialized,
- `E5405` index traversal integrity bound exceeded.

## Build

Requirements:

- CMake 3.24+
- C++20 compiler (MSVC, Clang, or GCC)

Configure and build:

```bash
cmake -S . -B build -D ATLASDB_BUILD_TESTS=ON
cmake --build build
```

## Run

Start REPL (in-memory):

```bash
./build/atlasdb_cli
```

Start REPL with persisted catalog snapshots:

```bash
./build/atlasdb_cli atlas.db
```

To use persisted catalog snapshots from C++ code, construct the engine with a database path:

```cpp
atlasdb::DatabaseEngine engine("atlas.db");
```

Example session:

```text
AtlasDB 0.1.0
Enter SQL-like statements. Use .exit to quit.
atlasdb> .version
ok: AtlasDB 0.1.0
atlasdb> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
ok: created table 'users'
atlasdb> INSERT INTO users VALUES (1, 'alice');
ok: inserted 1 row into 'users'
atlasdb> UPDATE users SET name = 'alicia' WHERE id = 1;
ok: updated 1 row in 'users'
atlasdb> SELECT * FROM users;
ok: selected 1 row(s) from 'users': [1, 'alicia']
atlasdb> DELETE FROM users WHERE id = 1;
ok: deleted 1 row from 'users'
atlasdb> .exit
```

## Test

Linux/macOS:

```bash
ctest --test-dir build --output-on-failure
```

Windows (multi-config generator):

```bash
ctest --test-dir build -C Debug --output-on-failure
```

## CI

CI is defined in `.github/workflows/ci.yml` and validates:

- Windows and Linux runners
- Debug and Release configurations
- Build and test execution for every push and pull request

## Extend

Recommended extension sequence:

1. Parser and AST expansion for CREATE and INSERT.
2. In-memory table catalog and row typing.
3. Pager-backed table row storage and row codecs (current persistence is catalog snapshots).
4. B+ tree index insertion, search, and split handling.
5. WAL and checkpoint recovery.

## Design Decisions and Tradeoffs

- C++20 is selected for strong type safety and modern language tooling.
- CMake keeps the build portable across Windows and Linux.
- Strict warnings keep quality high from the first commit.
- Feature rollout is phased to favor correctness over early complexity.
