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
- pager reopen persistence for page_count and page reads/writes.

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
