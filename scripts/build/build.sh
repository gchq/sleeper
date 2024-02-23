#!/bin/bash
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

BASE_DIR=$(cd "$(dirname "$0")" && cd "../../" && pwd)
MAVEN_DIR="$BASE_DIR/java"
SCRIPTS_DIR="$BASE_DIR/scripts"
JARS_DIR="$SCRIPTS_DIR/jars"
DOCKER_DIR="$SCRIPTS_DIR/docker"
VERSION_FILE="$SCRIPTS_DIR/templates/version.txt"

if [ "$#" -lt 1 ]; then
  MAVEN_PARAMS=(clean install -q -Pquick -T 1C)
else
  MAVEN_PARAMS=("$@")
fi

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_BUILD_TIME=$(record_time)
pushd "$MAVEN_DIR"

echo "-------------------------------------------------------------------------------"
echo "Building Project"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_BUILD_TIME")"

VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
SCRIPTS_DISTRIBUTION_DIR="$MAVEN_DIR/distribution/target/distribution-$VERSION-bin/scripts"
mvn "${MAVEN_PARAMS[@]}"

mkdir -p "$JARS_DIR"
mkdir -p "$DOCKER_DIR"
rm -rf "${JARS_DIR:?}"/*
rm -rf "${DOCKER_DIR:?}"/*
cp  "$SCRIPTS_DISTRIBUTION_DIR/jars"/* "$JARS_DIR"
cp -r "$SCRIPTS_DISTRIBUTION_DIR/docker"/* "$DOCKER_DIR"
cp  "$SCRIPTS_DISTRIBUTION_DIR/templates/version.txt" "$VERSION_FILE"

END_BUILD_TIME=$(record_time)
echo "Finished build at $(recorded_time_str "$END_BUILD_TIME"), took $(elapsed_time_str "$START_BUILD_TIME" "$END_BUILD_TIME")"

popd
