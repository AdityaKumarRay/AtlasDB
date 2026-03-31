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
- btree leaf-node tests now also cover deterministic leaf split distribution/link-preservation and split precondition failures (`E5106`).
- btree internal-node tests for initialize/append/insert/read/child-routing/left-child behavior, including root-split initialization, plus deterministic E5201/E5203/E5204/E5205/E5206 invariant failures.
- btree internal-node tests now also cover deterministic internal split redistribution/promotion behavior and split precondition failures (`E5207`), including propagation from leaf split metadata into internal routing.
- btree cursor tests for seek/seek-first/next traversal across linked leaves plus deterministic E5301/E5302/E5304/E5305 edge-path checks.
- btree index tests for initialize/open/insert/find, duplicate-key rejection, leaf-root split scans, and recursive split propagation through full internal parents with root growth checks.
- btree index seeded random-insert stress test that validates subtree key-range invariants and linked-leaf chain consistency after heavy split workloads.
- btree index reopen-and-continue-insert stress test that validates multi-level tree invariants and linked-leaf chain consistency across pager reopen boundaries.
- primary-index integration tests that validate ordered traversal and point key lookup by combining B+ tree index entries with table-store row payload reads/decodes.
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
