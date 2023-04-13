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

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd ../.. && pwd)
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
JARS_DIR="$SCRIPTS_DIR/jars"
GENERATED_DIR="$SCRIPTS_DIR/generated"

SYSTEM_TEST_JAR="$JARS_DIR/system-test-$VERSION-utility.jar"

INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Waiting for EMR clusters"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.bulkimport.WaitForEMRClusters "${INSTANCE_ID}"

END_EMR_TIME=$(record_time)
echo "EMR clusters terminated at $(recorded_time_str "$END_EMR_TIME"), took $(elapsed_time_str "$START_TIME" "$END_EMR_TIME")"

echo "-------------------------------------------------------------------------------"
echo "Checking all records have been written"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.bulkimport.CheckBulkImportRecords "${INSTANCE_ID}"

FINISH_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Finished waiting for bulk import"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "EMR clusters terminated at $(recorded_time_str "$END_EMR_TIME"), took $(elapsed_time_str "$START_TIME" "$END_EMR_TIME")"
echo "Checking output records finished at $(recorded_time_str "$END_EMR_TIME"), took $(elapsed_time_str "$END_EMR_TIME" "$FINISH_TIME")"
echo "Overall, took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
