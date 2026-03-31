# AtlasDB

AtlasDB is a production-oriented embedded database engine written in modern C++.

The project is inspired by incremental database-kernel tutorials, but its architecture and engineering process target portfolio-quality systems work: modular subsystems, deterministic behavior, durable storage primitives, and continuous testing.

## Current Status

Phase 0 foundation is in place:

- C++20 + CMake project layout.
- Strict compiler warning policy.
- GoogleTest test harness.
- REPL plus deterministic parser and in-memory execution for CREATE TABLE, INSERT, SELECT, UPDATE, and DELETE.
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

Start REPL:

```bash
./build/atlasdb_cli
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
3. Page-based persistence and row codecs.
4. B+ tree index insertion, search, and split handling.
5. WAL and checkpoint recovery.

## Design Decisions and Tradeoffs

- C++20 is selected for strong type safety and modern language tooling.
- CMake keeps the build portable across Windows and Linux.
- Strict warnings keep quality high from the first commit.
- Feature rollout is phased to favor correctness over early complexity.
