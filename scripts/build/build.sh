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
BASE_DIR=$(cd "$THIS_DIR" && cd "../../" && pwd)
MAVEN_DIR="$BASE_DIR/java"
SCRIPTS_DIR="$BASE_DIR/scripts"

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_BUILD_TIME=$(record_time)
pushd "$MAVEN_DIR"

echo "-------------------------------------------------------------------------------"
echo "Building Project"
echo "-------------------------------------------------------------------------------"
echo "Running Maven in quiet mode."
echo "For the first build, this should take up to 20-50 minutes. Subsequent builds should take 4-10 minutes."
echo "Rust compilation can be skipped to speed up the process by passing the argument -DskipRust."
echo "Started at $(recorded_time_str "$START_BUILD_TIME")"

mvn clean install  -Pquick -T 1C "$@"

"$THIS_DIR/copyBuildOutput.sh"

END_BUILD_TIME=$(record_time)
echo "Finished build at $(recorded_time_str "$END_BUILD_TIME"), took $(elapsed_time_str "$START_BUILD_TIME" "$END_BUILD_TIME")"

popd
