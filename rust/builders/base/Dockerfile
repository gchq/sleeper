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

FROM rust:1.93-trixie

RUN apt-get update && apt-get install -y \
    # Sleeper Rust build-time dependencies
    build-essential libssl-dev \
    # Clang installation dependencies
    lsb-release wget gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Clang 20
RUN wget -qO- https://apt.llvm.org/llvm.sh | bash -s -- 20 \
    && rm -rf /var/lib/apt/lists/*

ENV CC=clang-20
ENV CXX=clang++-20

WORKDIR /workspace
