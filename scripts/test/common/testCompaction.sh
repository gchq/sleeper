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
sleeper.systemtest.util.EnsureCompactionJobCreationPaused "$INSTANCE_ID"

END_CHECK_PAUSED_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Invoking compaction job creation"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.compaction.InvokeCompactionJobCreation "$INSTANCE_ID"

END_CREATE_COMPACTION_JOBS_TIME=$(record_time)
echo "Creating compaction jobs finished at $(recorded_time_str "$END_CREATE_COMPACTION_JOBS_TIME"), took $(elapsed_time_str "$END_CHECK_PAUSED_TIME" "$END_CREATE_COMPACTION_JOBS_TIME")"

echo "-------------------------------------------------------------------------------"
echo "Invoking compaction task creation"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.compaction.InvokeCompactionTaskCreation "$INSTANCE_ID"

END_CREATE_COMPACTION_TASKS_TIME=$(record_time)
echo "Creating compaction tasks finished at $(recorded_time_str "$END_CREATE_COMPACTION_TASKS_TIME"), took $(elapsed_time_str "$END_CREATE_COMPACTION_JOBS_TIME" "$END_CREATE_COMPACTION_TASKS_TIME")"

echo "-------------------------------------------------------------------------------"
echo "Waiting for compaction jobs"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.compaction.WaitForCompactionJobs "$INSTANCE_ID" "$TABLE_NAME"

java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.compaction.CompactionPerformanceValidator "$INSTANCE_ID" "$TABLE_NAME" "10" "4400000000" "300000"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished compaction test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Checking for paused state took $(elapsed_time_str "$START_TIME" "$END_CHECK_PAUSED_TIME")"
echo "Creating compaction jobs finished at $(recorded_time_str "$END_CREATE_COMPACTION_JOBS_TIME"), took $(elapsed_time_str "$END_CHECK_PAUSED_TIME" "$END_CREATE_COMPACTION_JOBS_TIME")"
echo "Creating compaction tasks finished at $(recorded_time_str "$END_CREATE_COMPACTION_TASKS_TIME"), took $(elapsed_time_str "$END_CREATE_COMPACTION_JOBS_TIME" "$END_CREATE_COMPACTION_TASKS_TIME")"
echo "Compaction finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_CREATE_COMPACTION_TASKS_TIME" "$FINISH_TIME")"
echo "Overall, took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
