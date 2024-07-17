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

unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
MAVEN_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <vpc> <csv-list-of-subnets> <results-bucket>"
  exit 1
fi

source "$SCRIPTS_DIR/functions/timeUtils.sh"
source "$SCRIPTS_DIR/functions/systemTestUtils.sh"

VPC=$1
SUBNETS=$2
RESULTS_BUCKET=$3
START_TIMESTAMP=$(record_time)
TEST_NAME=QuickSystemTests
OUTPUT_DIR="/tmp/sleeper/SystemTests/$START_TIMESTAMP"
SHORT_ID="i-$(uuidgen -t | cut -d'-' -f 1)"

mkdir -p "$OUTPUT_DIR"
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"

END_EXIT_CODE=0

runMavenQuickSystemTests() {
    echo "-------------------------------------------------------------------------------"
    echo "Running quick system tests"
    echo "-------------------------------------------------------------------------------"

    "$SCRIPTS_DIR/test/maven/buildDeployTest.sh" "$SHORT_ID" "$VPC" "$SUBNETS" > "$OUTPUT_DIR/$TEST_NAME.log"
    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        END_EXIT_CODE=$EXIT_CODE
    fi

    echo -n "$END_EXIT_CODE $SHORT_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
    echo -e "Test run complete \nSee $OUTPUT_DIR/$TEST_NAME.log for results"

    echo "-------------------------------------------------------------------------------"
    echo "Tearing down instance \"$SHORT_ID\" and \"$SHORT_ID-main\""
    echo "-------------------------------------------------------------------------------"
    "$SCRIPTS_DIR/test/maven/tearDown.sh" "$SHORT_ID" "$SHORT_ID-main" > "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
    echo "Tearing down for instance \"$SHORT_ID\" and \"$SHORT_ID-main\" complete"

    echo "-------------------------------------------------------------------------------"
    echo "[$(time_str)] Uploading test output"
    echo "-------------------------------------------------------------------------------"
    java -cp "${SYSTEM_TEST_JAR}" \
        "sleeper.systemtest.drivers.nightly.RecordNightlyTestOutput" "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

    echo "Upload complete"
}

runMavenQuickSystemTests

exit $END_EXIT_CODE
