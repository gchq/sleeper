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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

pushd "$THIS_DIR"/base
docker build -t sleeper-rust-builder-base:current .
popd

pushd "$THIS_DIR"/base-sccache
docker build -t sleeper-rust-builder-sccache:current .
popd

pushd "$THIS_DIR"/x86
docker build -t ghcr.io/gchq/sleeper-rust-builder-x86-sccache:latest --build-arg BASE_IMAGE=sleeper-rust-builder-sccache:current .
popd

pushd "$THIS_DIR"/graviton
docker build -t ghcr.io/gchq/sleeper-rust-builder-graviton-sccache:latest --build-arg BASE_IMAGE=sleeper-rust-builder-sccache:current .
popd
