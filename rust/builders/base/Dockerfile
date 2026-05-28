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

FROM rust:1.93-bookworm

# Copy certs/ if it exists, skip silently if not. The glob pattern cert[s]/ matches "certs/" when
# present but matches nothing (instead of failing) when absent — Docker's COPY fails on missing
# sources unless a glob is used.
COPY cert[s]/ /usr/local/share/ca-certificates/
# Only update CA certificates and switch to HTTPS sources if certs are present in ca-certificates dir
RUN [ -z "$(ls -A /usr/local/share/ca-certificates 2>/dev/null)" ] || \
    { update-ca-certificates && sed -i 's|http|https|g' /etc/apt/sources.list.d/debian.sources; }

RUN apt-get update && apt-get install -y \
    clang \
    && rm -rf /var/lib/apt/lists/*

ENV CC=clang
ENV CXX=clang++

WORKDIR /workspace
