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

RUN cargo install sccache --locked

ENV CC="sccache clang-20"
ENV CXX="sccache clang++-20"
ENV RUSTC_WRAPPER="/usr/local/cargo/bin/sccache"
ENV SCCACHE_DIR="/tmp/sccache"
