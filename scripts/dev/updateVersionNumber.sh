#!/usr/bin/env bash
# Copyright 2022-2024 Crown Copyright
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

NEW_VERSION=$1
echo "Setting new version to ${NEW_VERSION}"

# Update the version number in the pom.xml files in the java code
pushd "${PROJECT_ROOT}/java"
mvn versions:set -DnewVersion="${NEW_VERSION}" -DgenerateBackupPoms=false
popd

source "${PROJECT_ROOT}/scripts/functions/sedInPlace.sh"

# Update the version number in the Python module
NEW_VERSION_PYTHON="${NEW_VERSION//-SNAPSHOT/.dev1}"
sed_in_place \
  -e "s|^    version=.*|    version='${NEW_VERSION_PYTHON}',|" \
  "${PROJECT_ROOT}/python/setup.py"

# Update the version number in the Rust code
pushd "${PROJECT_ROOT}/rust"
cargo install cargo-edit
cargo set-version "${NEW_VERSION}"
popd
