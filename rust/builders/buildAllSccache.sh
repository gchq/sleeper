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
# If environment variables RUSTUP_DIST_SERVER or RUSTUP_UPDATE_ROOT are set, then expand them into a string like
# --build-arg RUSTUP_DIST_SERVER=${RUSTUP_SERVER} in BUILD_ARGS. If both are empty, then BUILD_ARGS is empty,
# otherwise, e.g. if RUSTUP_DIST_SERVER=http://example.com then BUILD_ARGS is "--build-arg RUSTUP_DIST_SERVER=http://example.com "
BUILD_ARGS="${RUSTUP_DIST_SERVER:+--build-arg RUSTUP_DIST_SERVER=${RUSTUP_DIST_SERVER} }${RUSTUP_UPDATE_ROOT:+--build-arg RUSTUP_UPDATE_ROOT=${RUSTUP_UPDATE_ROOT} }"

pushd "$THIS_DIR"/base
docker build -t sleeper-rust-builder-base:current .
popd

pushd "$THIS_DIR"/base-sccache
docker build -t sleeper-rust-builder-sccache:current .
popd

pushd "$THIS_DIR"/x86_64
docker build ${BUILD_ARGS} -t ghcr.io/gchq/sleeper-rust-builder-x86_64-sccache:latest --build-arg BASE_IMAGE=sleeper-rust-builder-sccache:current .
popd

pushd "$THIS_DIR"/aarch64
docker build ${BUILD_ARGS} -t ghcr.io/gchq/sleeper-rust-builder-aarch64-sccache:latest --build-arg BASE_IMAGE=sleeper-rust-builder-sccache:current .
popd
