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

set -e
unset CDPATH

BASE_DIR=$(cd "$(dirname "$0")" && cd "../../" && pwd)
MAVEN_DIR="$BASE_DIR/java"
SCRIPTS_DIR="$BASE_DIR/scripts"
JARS_DIR="$SCRIPTS_DIR/jars"
DOCKER_DIR="$SCRIPTS_DIR/docker"
VERSION_FILE="$SCRIPTS_DIR/templates/version.txt"

pushd "$MAVEN_DIR"
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
SCRIPTS_DISTRIBUTION_DIR="$MAVEN_DIR/distribution/target/distribution-$VERSION-bin/scripts"

mkdir -p "$JARS_DIR"
mkdir -p "$DOCKER_DIR"
rm -rf "${JARS_DIR:?}"/*
rm -rf "${DOCKER_DIR:?}"/*
cp  "$SCRIPTS_DISTRIBUTION_DIR/jars"/* "$JARS_DIR"
cp -r "$SCRIPTS_DISTRIBUTION_DIR/docker"/* "$DOCKER_DIR"
cp  "$SCRIPTS_DISTRIBUTION_DIR/templates/version.txt" "$VERSION_FILE"
popd
