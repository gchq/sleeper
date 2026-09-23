#!/usr/bin/env bash
# Copyright 2022-2026 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
BASE_DIR=$(cd "$THIS_DIR" && cd "../../" && pwd)

usage() {
  echo "Usage: $(basename "$0") [--with-sccache] [--image-prefix <prefix>]"
  echo "  --with-sccache          Also build the sccache builder image"
  echo "  --image-prefix <prefix> Prefix for image names, defaults to GitHub Container Registry if not set"
}

# This build creates multiple installs of the Rust and C/C++ toolchains, one per platform.
# These are tied together via configuration. There's also configuration to build in a custom environment.
#
# Here's a list of configuration that needs to apply when we build the builder image:
#
# - Rustup configuration to point to a custom install script and distribution/update server.
# - Which Rust toolchain version to install.
# - C/C++ toolchain to create sysroot for target platform of the image.
# - Clang compiler wrappers to use correct sysroot.
# - Cargo configured to use correct Clang for linking & compiling per target.
# - Sccache wrapper around correct Clang per target.
# - Image prefix for a custom publishing environment.
# - Per-platform Rust flags.
#
# Here's a list of configuration that needs to apply during a Rust build, via build-in-docker.sh:
#
# - Extra Cargo config to build in a custom environment (should not override any of the other configuration).
# - Sccache environment variables, some per-platform for cache configuration.
# - GitHub Actions cache publishing environment variables.

WITH_SCCACHE=false
IMAGE_PREFIX="ghcr.io/gchq"
DOCKER_OPTIONS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-sccache)
      WITH_SCCACHE=true
      shift
      ;;
    --image-prefix)
      if [[ $# -lt 2 ]]; then
        echo "--image-prefix needs a value"
        usage
        exit 1
      fi
      IMAGE_PREFIX="${2%/}" # Tolerate a trailing slash, as images are named "$IMAGE_PREFIX/..."
      shift 2
      ;;
    *)
      DOCKER_OPTIONS+=("$1")
      ;;
  esac
done

# Only add the separating slash when a prefix is set, so an empty prefix gives an unprefixed image name
IMAGE_PATH_PREFIX="${IMAGE_PREFIX:+$IMAGE_PREFIX/}"
BASE_IMAGE="${IMAGE_PATH_PREFIX}sleeper-rust-builder-al2023:latest"
SCCACHE_IMAGE="${IMAGE_PATH_PREFIX}sleeper-rust-builder-sccache:latest"

# If environment variables are set, then expand them into a string like
# --build-arg RUSTUP_DIST_SERVER=${RUSTUP_SERVER} in BUILD_ARGS. If all are empty, then BUILD_ARGS is empty,
# otherwise, e.g. if RUSTUP_DIST_SERVER=http://example.com then BUILD_ARGS is "--build-arg RUSTUP_DIST_SERVER=http://example.com "
BUILD_ARGS="${RUSTUP_INIT_URL:+--build-arg RUSTUP_INIT_URL=${RUSTUP_INIT_URL} }${RUSTUP_DIST_SERVER:+--build-arg RUSTUP_DIST_SERVER=${RUSTUP_DIST_SERVER} }${RUSTUP_UPDATE_ROOT:+--build-arg RUSTUP_UPDATE_ROOT=${RUSTUP_UPDATE_ROOT} }"

pushd "$THIS_DIR"/base
rm -rf certs
# Copy custom CA certs into build context if present at repo root.
# Ignore README.md — the certs directory is checked into Git via a placeholder README,
# so we only treat the directory as populated when it contains at least one other file.
if [ -n "$(ls -A "$BASE_DIR/certs" 2>/dev/null | grep -v '^README\.md$')" ]; then
  cp -r "$BASE_DIR/certs" certs
  rm -f certs/README.md
fi
docker build ${BUILD_ARGS} -t "$BASE_IMAGE" "${DOCKER_OPTIONS[@]}" .
popd

if [[ "$WITH_SCCACHE" == "true" ]]; then
  pushd "$THIS_DIR"/sccache
  # Pass the base image explicitly so this builds on the image we just built, not the default in the Dockerfile
  docker build --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$SCCACHE_IMAGE" "${DOCKER_OPTIONS[@]}" .
  popd
fi
