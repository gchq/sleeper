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
PROJECT_ROOT=$(dirname "$(dirname "${THIS_DIR}")")

echo "Checking versions:"

# Get the version number in the pom.xml files in the java code
pushd "${PROJECT_ROOT}/java"
JAVA_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "Java version: $JAVA_VERSION"
popd

# Get the version number in the Python module
pushd "${PROJECT_ROOT}/python"
PYTHON_VERSION=$(python3 -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])")
echo "Python version: $PYTHON_VERSION"
popd

# Get the version number in the Rust code
pushd "${PROJECT_ROOT}/rust"
RUST_VERSION=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
echo "Rust version: $RUST_VERSION"
popd

# Check Java and Rust as they should be identical
if [ $JAVA_VERSION != $RUST_VERSION ]; then
    echo "The Java and Rust versions do not match. Java version: $JAVA_VERSION Rust version: $RUST_VERSION"
    exit 1
fi

PYTHON_CHECK_VERSION="${JAVA_VERSION//-SNAPSHOT/.dev1}"

# Check version number against Python version
if [ $PYTHON_CHECK_VERSION != $PYTHON_VERSION ]; then
    echo "Python version number is $PYTHON_VERSION but should be $PYTHON_CHECK_VERSION"
    exit 1
fi
