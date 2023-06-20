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
THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
JARS_DIR="$SCRIPTS_DIR/jars"
TEMPLATE_DIR="$SCRIPTS_DIR/templates"
GENERATED_DIR="$SCRIPTS_DIR/generated"

VERSION=$(cat "$TEMPLATE_DIR/version.txt")
SYSTEM_TEST_JAR="${JARS_DIR}/system-test-${VERSION}-utility.jar"
WRITE_DATA_OUTPUT_FILE="$GENERATED_DIR/writeDataOutput.json"

INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Writing Random Data"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.ingest.RunWriteRandomDataTaskOnECS "${INSTANCE_ID}" "${TABLE_NAME}" "${WRITE_DATA_OUTPUT_FILE}"

END_RUN_TASKS=$(record_time)
echo "Starting data generation tasks took $(elapsed_time_str "$START_TIME" "$END_RUN_TASKS")"

"$THIS_DIR/waitForDataGeneration.sh"

END_DATA_GENERATION=$(record_time)

"$THIS_DIR/waitForBulkImport.sh"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished bulk import"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Starting data generation tasks took $(elapsed_time_str "$START_TIME" "$END_RUN_TASKS")"
echo "Waiting for data generation took $(elapsed_time_str "$END_RUN_TASKS" "$END_DATA_GENERATION")"
echo "Bulk import finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_DATA_GENERATION" "$FINISH_TIME")"
echo "Overall, test took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
