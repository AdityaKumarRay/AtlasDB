# AtlasDB Testing Strategy

## Test Categories

- Unit tests
  - parser, catalog, page/header codecs, pager, and engine statement behavior.
- Integration tests
  - end-to-end statement execution and persistence round trips.
- Recovery tests
  - crash-injection and WAL replay correctness.

Current implemented persistence-focused coverage:

- restart round-trip for persisted catalog snapshots,
- deterministic startup failure on corrupted snapshot magic,
- pager reopen persistence for page_count and page reads/writes,
- catalog snapshot codec unit tests for round-trip, null output buffer, unsupported version, and trailing-byte rejection,
- row codec unit tests for round-trip plus E3201/E3202/E3204/E3205/E3206 deterministic decode/encode failures.
- slotted row-page tests for append/read/count behavior plus E3301/E3303/E3304/E3305 structural validation failures.
- table-store tests for initialize/append/read/row-count, scan ordering across multi-page growth, and deterministic E3402/E3404/E3405 path checks.
- btree leaf-node tests for initialize/append/read/find/next-page behavior plus deterministic E5101/E5103/E5104/E5105 invariant failures.
- btree internal-node tests for initialize/append/read/child-routing/left-child behavior plus deterministic E5201/E5203/E5204/E5205 invariant failures.
- engine persistence regression test covering UPDATE/DELETE followed by SELECT across restart with page-native scan decoding.
- engine persistence regression test covering multi-row INSERT order across restart with table-store-backed SELECT decoding.
- engine persistence regression test covering CREATE of a new table without disturbing existing-table rows across restart.
- engine persistence regression test covering UPDATE on one table while preserving another table's rows across restart.
- engine persistence regression test covering DELETE on one table while preserving another table's rows across restart.

## Execution

Configure and build tests:

```bash
cmake -S . -B build -D ATLASDB_BUILD_TESTS=ON
cmake --build build
```

Run tests:

```bash
ctest --test-dir build --output-on-failure
```

For multi-config generators (such as Visual Studio on Windows), pass a config:

```bash
ctest --test-dir build -C Debug --output-on-failure
```

## Continuous Integration

GitHub Actions workflow: `.github/workflows/ci.yml`

Matrix:

- ubuntu-latest + Debug
- ubuntu-latest + Release
- windows-latest + Debug
- windows-latest + Release

Each job performs configure, build, and test.

## Quality Rules

- Tests must be deterministic and non-flaky.
- Every bug fix adds a regression test.
- CI should fail on warnings, compile errors, or test failures.
- New deterministic error codes should be accompanied by direct assertion tests.
