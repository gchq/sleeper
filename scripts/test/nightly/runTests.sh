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
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
MAVEN_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)

pushd "$SCRIPTS_DIR/test"

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <vpc> <csv-list-of-subnets> <results-bucket> <optional-test-type> <optional-maven-parameters>"
  echo "Valid test types are: performance, functional"
  exit 1
fi

VPC=$1
SUBNETS=$2
RESULTS_BUCKET=$3
MAIN_SUITE_NAME=$4
shift 3
if [ "$MAIN_SUITE_NAME" == "performance" ]; then
  shift
  MAIN_SUITE_PARAMS=(-Dsleeper.system.test.cluster.enabled=true -DrunIT=NightlyPerformanceSystemTestSuite "$@")
elif [ "$MAIN_SUITE_NAME" == "functional" ]; then
  shift
  MAIN_SUITE_PARAMS=(-DrunIT=NightlyFunctionalSystemTestSuite "$@")
elif [ "$1" == "--main" ]; then
  MAIN_SUITE_NAME=custom
  MAIN_SUITE_PARAMS=("$2")
  shift 2
else
  MAIN_SUITE_NAME=custom
  MAIN_SUITE_PARAMS=("$@")
fi
SECONDARY_SUITE_NAME=dynamo-state-store
SECONDARY_SUITE_PARAMS=(-Dsleeper.system.test.force.statestore.classname=sleeper.statestore.dynamodb.DynamoDBStateStore "$@")

echo "MAIN_SUITE_NAME=$MAIN_SUITE_NAME"
echo "MAIN_SUITE_PARAMS=(${MAIN_SUITE_PARAMS[*]})"
echo "SECONDARY_SUITE_NAME=$SECONDARY_SUITE_NAME"
echo "SECONDARY_SUITE_PARAMS=(${SECONDARY_SUITE_PARAMS[*]})"

source "$SCRIPTS_DIR/functions/timeUtils.sh"
source "$SCRIPTS_DIR/functions/systemTestUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
START_DAY=$(recorded_time_str "$START_TIMESTAMP" "%m%d")
# Use randomness to avoid naming conflicts between test runs
DAY_RUN_ID=$(uuidgen -r | head -c 4)
TEST_RUN_ID="$START_DAY-$DAY_RUN_ID"
OUTPUT_DIR="/tmp/sleeper/${MAIN_SUITE_NAME}Tests/$START_TIME"

mkdir -p "$OUTPUT_DIR"
../build/buildForTest.sh
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"
set +e

END_EXIT_CODE=0

runMavenSystemTests() {
    SHORT_ID=$1
    TEST_NAME=$2
    shift 2
    TEST_EXIT_CODE=0
    EXTRA_MAVEN_PARAMS=("$@")
    TEST_OUTPUT_DIR="$OUTPUT_DIR/$TEST_NAME"
    mkdir "$TEST_OUTPUT_DIR"
    pushd "$MAVEN_DIR"
    mvn clean
    popd
    ./maven/deployTest.sh "$SHORT_ID" "$VPC" "$SUBNETS" \
      -Dsleeper.system.test.output.dir="$TEST_OUTPUT_DIR" \
      "${EXTRA_MAVEN_PARAMS[@]}" \
      &> "$OUTPUT_DIR/$TEST_NAME.log"
    RUN_TESTS_EXIT_CODE=$?
    if [ $RUN_TESTS_EXIT_CODE -ne 0 ]; then
      END_EXIT_CODE=$RUN_TESTS_EXIT_CODE
      TEST_EXIT_CODE=$RUN_TESTS_EXIT_CODE
    fi
    pushd "$MAVEN_DIR"
    mvn --batch-mode site site:stage -pl system-test/system-test-suite \
       -DskipTests=true \
       -DstagingDirectory="$TEST_OUTPUT_DIR/site"
    popd
    pushd "$TEST_OUTPUT_DIR/site"
    zip -r "$OUTPUT_DIR/$TEST_NAME-site.zip" "."
    popd
    rm -rf "$TEST_OUTPUT_DIR/site"
    SHORT_INSTANCE_NAMES=$(read_short_instance_names_from_instance_ids "$SHORT_ID" "$TEST_OUTPUT_DIR/instanceIds.txt")
    ./maven/tearDown.sh "$SHORT_ID" "$SHORT_INSTANCE_NAMES" &> "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
    TEARDOWN_EXIT_CODE=$?
    if [ $TEARDOWN_EXIT_CODE -ne 0 ]; then
      TEST_EXIT_CODE=$TEARDOWN_EXIT_CODE
      END_EXIT_CODE=$TEARDOWN_EXIT_CODE
    fi
    echo -n "$TEST_EXIT_CODE $SHORT_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
}

runMavenSystemTests "mvn-$TEST_RUN_ID" $MAIN_SUITE_NAME "${MAIN_SUITE_PARAMS[@]}"
runMavenSystemTests "dyn-$TEST_RUN_ID" $SECONDARY_SUITE_NAME "${SECONDARY_SUITE_PARAMS[@]}"

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.nightly.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd

exit $END_EXIT_CODE
