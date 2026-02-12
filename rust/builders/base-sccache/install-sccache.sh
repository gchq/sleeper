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

SCCACHE_REPO="https://github.com/mozilla/sccache"

case "$(uname -m)" in
  x86_64|amd64)
    ARCH="x86_64"
    ;;
  aarch64|arm64)
    ARCH="aarch64"
    ;;
  *)
    echo "Unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

ARCH_TRIPLE="${ARCH}-unknown-linux-musl"

TEMP_DIR="$(mktemp -d)"
pushd "$TEMP_DIR"

SCCACHE_TAG=$(git ls-remote --tags --refs --exit-code \
    "$SCCACHE_REPO" \
    | cut -d/ -f3 \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort --version-sort \
    | tail -n1)
curl -LSfs "$SCCACHE_REPO/releases/download/$SCCACHE_TAG/sccache-$SCCACHE_TAG-$ARCH_TRIPLE.tar.gz" \
    -o sccache.tar.gz

tar -xvf sccache.tar.gz
rm sccache.tar.gz
cp "sccache-$SCCACHE_TAG-$ARCH_TRIPLE/sccache" "/usr/bin/sccache"
chmod +x "/usr/bin/sccache"

popd
rm -rf "$TEMP_DIR"
