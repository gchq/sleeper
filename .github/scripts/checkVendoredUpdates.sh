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

set -e
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
BASE_DIR=$(cd "$THIS_DIR" && cd "../../" && pwd)

pushd "$BASE_DIR/vendored/datasketches-cpp"
CURRENT_VERSION=$(ls)
git clone --depth 1 https://github.com/apache/datasketches-cpp.git latest
pushd latest
git fetch --tags
LATEST_VERSION=$(git tag -l --sort=-version:refname | grep -P '\.[0-9]+$' | head -1)
popd
rm -rf ./latest
popd

echo "Found current version: $CURRENT_VERSION"
echo "Found latest version: $LATEST_VERSION"
if [ "$CURRENT_VERSION" != "$LATEST_VERSION" ]; then
    echo "Versions do not match"
    exit 1
else
    echo "Versions match"
fi
