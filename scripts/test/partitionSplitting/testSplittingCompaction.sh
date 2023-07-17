#!/usr/bin/env bash
# Copyright 2022-2023 Crown Copyright
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

TABLE_NAME="system-test"
SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd ../.. && pwd)
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
JARS_DIR="$SCRIPTS_DIR/jars"
GENERATED_DIR="$SCRIPTS_DIR/generated"

SYSTEM_TEST_JAR="$JARS_DIR/system-test-$VERSION-utility.jar"

INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.util.EnsureCompactionJobCreationPaused "$INSTANCE_ID"

END_CHECK_PAUSED_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Splitting partitions"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.compaction.SplitPartitionsUntilNoMoreSplits "$INSTANCE_ID" "$TABLE_NAME"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished splitting test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Checking for paused state took $(elapsed_time_str "$START_TIME" "$END_CHECK_PAUSED_TIME")"
echo "Finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
