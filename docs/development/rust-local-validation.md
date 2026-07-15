Rust local validation
=====================

The Rust workspace is under `rust/`.

The workspace uses the Rust toolchain version declared in `rust/Cargo.toml`. Before running the validation commands,
ensure your local toolchain matches the version declared there.

You can verify the active toolchain with:

```bash
rustc --version
cargo --version
```

The Rust workspace can be developed locally on Linux, WSL or macOS, or in the repository development container. These
environments use the same Rust toolchain workflow.

A useful focused integration test for the Rust data-processing path is:

```bash
cargo test -p sleeper_core --test compaction_test
```

This runs locally without AWS. It writes small local Parquet inputs, runs DataFusion compaction, and checks output rows,
row counts, filtering, aggregation, progress callback behaviour, and sketch output.

## Rust toolchain

### Linux, WSL and macOS

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

On lower-spec systems, full workspace compilation may consume substantial CPU and memory before any tests begin. If
required, reduce Cargo build parallelism:

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

The repository development container provides the supported toolchain environment. It is configured in
`.devcontainer/devcontainer.json`; see the [Developer Guide](../developer-guide.md) for the VS Code dev-container
workflow. Once the repository is open inside the container, run the same validation commands described above from
`rust/`.

## AWS or deployed instance

Deployed-instance validation is covered by the [system testing documentation](system-tests.md) and manual testing flows.
Use it for behaviour that cannot be proved locally, including IAM, AWS networking, S3, deployed state stores, Lambda,
ECS, EMR, and full Sleeper orchestration.
