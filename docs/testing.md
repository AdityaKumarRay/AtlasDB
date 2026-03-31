# AtlasDB Testing Strategy

## Test Categories

- Unit tests
  - parser, catalog, codec, B+ tree nodes, WAL records.
- Integration tests
  - end-to-end statement execution and persistence round trips.
- Recovery tests
  - crash-injection and WAL replay correctness.

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
