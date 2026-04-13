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

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <new-version-number>"
    exit 1
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$(dirname "${THIS_DIR}")")
DATASKETCHES_VENDORED_DIR="${PROJECT_ROOT}/vendored/datasketches-cpp/"

NEW_VERSION=$1
echo "${DATASKETCHES_VENDORED_DIR} Updating Apache DataSketches CPP to version: ${NEW_VERSION}"

# Remove version already present if directory not-empty
if [ ! -z "$(find "${DATASKETCHES_VENDORED_DIR}" -mindepth 1 -print -quit 2>/dev/null)" ]; then
    echo "Deleting current version"
    # find "${DATASKETCHES_VENDORED_DIR}" -mindepth 1 -delete
fi

# Retrieve the named version and place in correct directory
git clone --depth 1 --branch "${NEW_VERSION}" https://github.com/apache/datasketches-cpp.git /tmp/datasketches-cpp
rm -rf /tmp/datasketches-cpp/.git
mv /tmp/datasketches-cpp "${DATASKETCHES_VENDORED_DIR}"

source "${PROJECT_ROOT}/scripts/functions/sedInPlace.sh"

# Update the version number in the Rust Cargo.toml file
sed_in_place \
  -e "s|^datasketches_cpp_version = .*|datasketches_cpp_version = \"${NEW_VERSION}\"|" \
  "${PROJECT_ROOT}/rust/Cargo.toml"
