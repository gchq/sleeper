Rust local validation
=====================

The Rust workspace is under `rust/`.

For general Rust development, use WSL/Linux or the repository development container. These are the recommended local
environments. Native Windows can still be useful for targeted portability checks, but it is not a generally supported
development environment.

A useful focused integration test for the Rust data-processing path is:

```bash
cargo test -p sleeper_core --test compaction_test
```

This runs without Docker or AWS. It writes small local Parquet inputs, runs DataFusion compaction, and checks output
rows, row counts, filtering, aggregation, progress callback behaviour, and sketch output.

## Recommended local environments

### WSL or Linux

Run the following commands from `rust/`.

The workspace includes the DataSketches C++ bridge used by `rust_sketch`, so the environment must provide a working
C/C++ build toolchain. CMake and `pkg-config` may also be required.

For formatting, linting, and dependency advisory checks:

```bash
cargo audit
cargo fmt --all -- --check
cargo clippy --no-deps --all-targets -- -W clippy::pedantic -D warnings
```

For focused package validation:

```bash
cargo test -p rust_sketch
cargo test -p filter_udfs
cargo test -p sleeper_core
cargo test -p sleeper_df
cargo test -p objectstore_ext
```

For full workspace validation:

```bash
cargo test
```

Full workspace validation can put substantial pressure on some systems during parallel compilation, before any tests
begin. If memory use or system responsiveness becomes a problem, reduce Cargo build parallelism:

```bash
cargo test -j 1
```

This limits Cargo to one build job at a time. It makes compilation slower, but reduces the number of compiler and
linker processes running concurrently. It does not force tests within each test binary to run serially.

`cargo audit` is the dependency advisory check used by CI. To inspect duplicate dependency versions, run:

```bash
cargo tree -d
```

Duplicates introduced by the DataFusion, Arrow, AWS SDK, or build-tool dependency graphs should be reviewed before
being treated as actionable problems.

### Development container

The repository development container provides a consistent local environment for Rust development. It is configured
in `.devcontainer/devcontainer.json`; see the [Developer Guide](../developer-guide.md) for the VS Code dev-container
workflow.

Once the repository is open inside the container, run the same validation commands described in the WSL or Linux
section from `rust/`.

## Native Windows, targeted validation only

Native Windows is not the recommended environment for general Sleeper development. It can still be useful for focused
Rust validation and for identifying portability issues.

Run commands from `rust/` in PowerShell or another Windows shell. Use the Rust version declared by the repository and
install a working MSVC C++ build toolchain, because the workspace builds the DataSketches C++ bridge used by
`rust_sketch`.

Useful targeted checks include:

```powershell
cargo audit
cargo tree -d
cargo fmt --all -- --check
cargo clippy --no-deps --all-targets -- -W clippy::pedantic -D warnings

cargo test -p rust_sketch
cargo test -p filter_udfs
cargo test -p sleeper_core
cargo test -p sleeper_df
cargo test -p objectstore_ext
cargo test -p apps --test compact_cli --test query_cli
```

For a focused integration check of the Rust data-processing path:

```powershell
cargo test -p sleeper_core --test compaction_test
```

A full workspace run on native Windows may place significant pressure on the system during parallel compilation. When
necessary, reduce Cargo build parallelism:

```powershell
cargo test -j 1
```

Treat native Windows results as targeted portability evidence. Confirm broader development and validation behaviour in
WSL/Linux or the repository development container.

## Docker

Docker validation is a broader tier after the local Rust path is working. Run these commands from the repository root:

```bash
./rust/build-in-docker.sh x86_64 cargo test
./rust/build-in-docker.sh x86_64 cargo test -p sleeper_core --test compaction_test
./scripts/test/docker/testCompactionDockerImage.sh
```

The Docker image test builds and checks the compaction job execution image. This is broader than the Rust-only local
path and requires Docker.

## AWS or deployed instance

Deployed-instance validation is covered by the system test suite and manual testing flows. Use it for behaviour that
cannot be proved locally, including IAM, AWS networking, S3, deployed state stores, Lambda, ECS, EMR, and full Sleeper
orchestration.