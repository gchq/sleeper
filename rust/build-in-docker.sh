#!/usr/bin/env bash
# Copyright 2022-2025 Crown Copyright
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

PROJECT_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)

PLATFORM=$1
shift
if [ "$PLATFORM" = "x86_64" ]; then
  BUILD_IMAGE=${RUST_BUILD_IMAGE_X86_64:-"ghcr.io/gchq/sleeper-rust-builder-x86_64:latest"}
elif [ "$PLATFORM" = "aarch64" ]; then
  BUILD_IMAGE=${RUST_BUILD_IMAGE_AARCH64:-"ghcr.io/gchq/sleeper-rust-builder-aarch64:latest"}
else
  echo "Platform not recognised, expected x86_64 or aarch64: $PLATFORM"
  exit 1
fi

if [[ -z $1 ]]; then
  BUILD_COMMAND=(cargo build --release --package sleeper_df)
else
  BUILD_COMMAND=("$@")
fi

if [ "$IN_CLI_CONTAINER" = "true" ]; then
  PATH_IN_MOUNT="${PROJECT_DIR#$CONTAINER_MOUNT_PATH}"
  MOUNT_DIR="$HOST_MOUNT_PATH/$PATH_IN_MOUNT"
else
  MOUNT_DIR="$PROJECT_DIR"
fi

RUN_PARAMS=()
if [ -t 1 ]; then # Only pass TTY to Docker if connected to terminal
  RUN_PARAMS+=(-it)
fi
RUN_PARAMS+=(
  --rm
  -v "$MOUNT_DIR":/workspace
  -w /workspace/rust
  -e SCCACHE_ERROR_LOG
  -e SCCACHE_LOG
  -e SSCACHE_CACHE_SIZE
  -e SCCACHE_GHA_ENABLED
  -e ACTIONS_CACHE_URL
  -e ACTIONS_RESULTS_URL
  -e ACTIONS_RUNTIME_TOKEN
  -e ACTIONS_CACHE_SERVICE_V2
  "$BUILD_IMAGE"
)

docker pull "$BUILD_IMAGE"
docker run "${RUN_PARAMS[@]}" "${BUILD_COMMAND[@]}"
