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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd .. && pwd)
BASE_DIR=$(cd "$SCRIPTS_DIR" && cd .. && pwd)
MAVEN_DIR="$BASE_DIR/java"
MAVEN_MODULE_DIR="$MAVEN_DIR/trino"
SCRIPTS_DIR="$BASE_DIR/scripts"
JARS_DIR="$THIS_DIR/jars"
ETC_DIR="$THIS_DIR/etc"

if [ "$#" -lt 1 ]; then
  MAVEN_PARAMS=(clean install -q -Pquick)
else
  MAVEN_PARAMS=("$@")
fi

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Building trino module"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"

pushd "$MAVEN_MODULE_DIR"
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
mvn "${MAVEN_PARAMS[@]}"
popd

mkdir -p "$JARS_DIR"
rm -rf "${JARS_DIR:?}"/*
cp "$MAVEN_MODULE_DIR/target/trino-$VERSION-utility.jar" "$JARS_DIR/sleeper-trino-plugin.jar"

mkdir -p "$ETC_DIR"
rm -rf "${ETC_DIR:?}"/*
cp -r "$MAVEN_MODULE_DIR/example.trino.etc.dir"/* "$ETC_DIR"

END_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished build"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Finished at $(recorded_time_str "$END_TIME"), took $(elapsed_time_str "$START_TIME" "$END_TIME")"
