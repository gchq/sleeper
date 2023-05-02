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
WRITE_DATA_OUTPUT_FILE="$GENERATED_DIR/writeDataOutput.json"

INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Waiting for tasks to generate data"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.ingest.WaitForGenerateData "${WRITE_DATA_OUTPUT_FILE}"

END_DATA_GENERATION=$(record_time)
echo "Waiting for data generation finished at $(recorded_time_str "$END_DATA_GENERATION"), took $(elapsed_time_str "$START_TIME" "$END_DATA_GENERATION")"

echo "-------------------------------------------------------------------------------"
echo "Sending bulk import jobs"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.bulkimport.SendBulkImportJobs "${INSTANCE_ID}" "${TABLE_NAME}"

FINISH_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Finished generating data and sending bulk import jobs"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Waiting for data generation finished at $(recorded_time_str "$END_DATA_GENERATION"), took $(elapsed_time_str "$START_TIME" "$END_DATA_GENERATION")"
echo "Sending bulk import jobs finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_DATA_GENERATION" "$FINISH_TIME")"
echo "Overall, took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
