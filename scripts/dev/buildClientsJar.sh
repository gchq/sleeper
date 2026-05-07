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
JAVA_DIR="${PROJECT_ROOT}/java"
SCRIPTS_DIR="${PROJECT_ROOT}/scripts"
JARS_DIR="${SCRIPTS_DIR}/jars"
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

pushd "$JAVA_DIR"
mvn package -Pquick -pl clients -am -DskipRust
popd

cp "$JAVA_DIR/clients/target/clients-$VERSION-utility.jar" "$JARS_DIR/clients-$VERSION-utility.jar"
