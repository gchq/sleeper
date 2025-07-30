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

ARG BASE_IMAGE="sleeper-rust-builder-base:current"
FROM ${BASE_IMAGE}

RUN apt-get install --assume-yes --no-install-recommends \
    g++-aarch64-linux-gnu \
    libc6-dev-arm64-cross

RUN rustup target add aarch64-unknown-linux-gnu
RUN rustup toolchain install stable-aarch64-unknown-linux-gnu --profile minimal --force-non-host

ENV RUSTFLAGS="-Ctarget-feature=+lse -Ctarget-cpu=neoverse-n1 -Clinker=aarch64-linux-gnu-gcc"
ENV CARGO_BUILD_TARGET="aarch64-unknown-linux-gnu"
