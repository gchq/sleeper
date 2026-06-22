Building Sleeper in a custom environment
========================================

Sleeper's build and deployment scripts have a small set of configuration points for users whose environments need to substitute the default build inputs. This page lists every such point in one place: what it does, and a worked example.

None of these are needed for a default build.

## Container images

### `--override-base-image-dir` (on `setDeployConfig.sh` and `publishDocker.sh`)

Overrides the directory used as the Docker build context for the Sleeper base Docker image. This image is used as the base image for most of the other Docker images built during a Sleeper deployment. By default the base image is built from `scripts/docker/base`. Pass a different directory containing your own `Dockerfile` to substitute a custom base image.

The override only applies to local builds — it cannot be combined with `--image-location repository`.

Example — `setDeployConfig.sh` (persists the override in `scripts/templates/deployConfig.json`, so it is picked up by `deployNew.sh` and `deployExisting.sh`):

```bash
./scripts/deploy/setDeployConfig.sh \
    --image-location localBuild \
    --override-base-image-dir /path/to/my/base
```

Example — `publishDocker.sh` (override at publish time only; saved config is not consulted):

```bash
./scripts/dev/publishDocker.sh my.registry.com/path true /path/to/my/base
```

See [publishing artefacts](publishing.md) for the full publish flow.

## Rust build

The Rust components are built inside Docker containers via `rust/build-in-docker.sh` (this script is called in-directly from Maven during a normal Sleeper build process), with the builder images themselves produced by `rust/builders/buildAll.sh` (or `buildAllSccache.sh`). The configuration points below affect either the building of those builder images or how the Rust workload runs inside them.

The variables below should be set via environment variables as shown.

### `RUSTUP_DIST_SERVER` and `RUSTUP_UPDATE_ROOT`

Override the upstream servers used by `rustup` when installing the Rust toolchain into the builder images.

Example:

```bash
export RUSTUP_DIST_SERVER=https://rustup.internal.example.com
export RUSTUP_UPDATE_ROOT=https://rustup.internal.example.com/rustup
./rust/builders/buildAll.sh
```

Either variable can be set independently. If both are unset, the builder images are built against the public rustup servers.

### `EXTRA_CARGO_CONFIG`

Appends arbitrary TOML to the `cargo` config used for the current Rust build, without modifying the checked-in `rust/.cargo/config.toml`. The value must be valid TOML; multi-line values are supported via `\n` escapes.

Use this to point `cargo` at an internal `crates.io` mirror, configure a private registry, set source replacement rules, or any other per-environment `cargo` setting that should not be committed.

Example — replace the default `crates.io` source with an internal mirror:

```bash
export EXTRA_CARGO_CONFIG='[source.crates-io]\nreplace-with = "internal-mirror"\n\n[source.internal-mirror]\nregistry = "https://crates.internal.example.com/index"'
./rust/build-in-docker.sh x86_64
```

### `SKIP_DOCKER_PULL`

When set to any non-empty value, `rust/build-in-docker.sh` does not run `docker pull` against the builder image before running the build. The build still uses whatever image is locally tagged.

Useful when you have built the builder image locally and do not want it overwritten by a pull, or when the public registry is unreachable and the image has been loaded by some other means.

Example:

```bash
./rust/builders/buildAll.sh                # builds and tags the image locally
export SKIP_DOCKER_PULL=true
./rust/build-in-docker.sh x86_64           # uses the local image, no pull
```

### `certs/` directory

Drop PEM-encoded CA certificate files into the `certs/` directory at the repository root if the Rust builder image needs to trust a private certificate authority. Any file extension may be used — `.crt`, `.pem`, `.cer`, etc.

Files placed here (other than the placeholder `README.md`) are picked up by `rust/builders/buildAll.sh` and `rust/builders/buildAllSccache.sh` and installed as trusted CA certificates **inside the builder container**. Nothing is installed on the host.

If `certs/` only contains the placeholder `README.md`, the builder images are built without any custom CA trust changes.

Example:

```bash
cp my-corporate-root-ca.crt certs/
./rust/builders/buildAll.sh
```
