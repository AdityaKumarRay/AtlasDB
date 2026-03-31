# AtlasDB

AtlasDB is a production-oriented embedded database engine written in modern C++.

The project is inspired by incremental database-kernel tutorials, but its architecture and engineering process target portfolio-quality systems work: modular subsystems, deterministic behavior, durable storage primitives, and continuous testing.

## Current Status

Phase 0 foundation is in place:

- C++20 + CMake project layout.
- Strict compiler warning policy.
- GoogleTest test harness.
- REPL plus deterministic parser and in-memory execution for CREATE TABLE, INSERT, SELECT, UPDATE, and DELETE.
- Storage foundation includes page/header codecs plus pager file I/O and page allocation primitives.
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
