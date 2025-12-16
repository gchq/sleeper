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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
BASE_DIR=$(cd "$SCRIPTS_DIR" && cd .. && pwd)
MAVEN_DIR="$BASE_DIR/java"
ENVIRONMENT_MAVEN_DIR="$MAVEN_DIR/deployment/cdk-environment"
BUILD_UPTIME_MAVEN_DIR="$MAVEN_DIR/deployment/build-uptime-lambda"
SCRIPTS_DIR="$BASE_DIR/scripts"
VERSION_FILE="$THIS_DIR/version.txt"
JARS_DIR="$THIS_DIR/jars"

if [ "$#" -lt 1 ]; then
  MAVEN_PARAMS=(clean install -q -Pquick)
else
  MAVEN_PARAMS=("$@")
fi

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Building Java code"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"

pushd "$MAVEN_DIR"
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
mvn -pl deployment/build-uptime-lambda,deployment/cdk-environment -am "${MAVEN_PARAMS[@]}"
popd

echo "$VERSION" > "$VERSION_FILE"
mkdir -p "$JARS_DIR"
rm -rf "${JARS_DIR:?}"/*
cp "$ENVIRONMENT_MAVEN_DIR/target/cdk-environment-$VERSION-utility.jar" "$JARS_DIR/cdk-environment.jar"
cp "$BUILD_UPTIME_MAVEN_DIR/target/build-uptime-lambda-$VERSION-utility.jar" "$JARS_DIR/build-uptime-lambda.jar"

END_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished build"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Finished at $(recorded_time_str "$END_TIME"), took $(elapsed_time_str "$START_TIME" "$END_TIME")"
