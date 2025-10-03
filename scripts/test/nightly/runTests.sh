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
MAVEN_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)
REPO_PARENT_DIR=$(cd "$SCRIPTS_DIR" && cd ../.. && pwd)

pushd "$SCRIPTS_DIR/test"

if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <deploy-id> <vpc> <csv-list-of-subnets> <results-bucket> <optional-test-type> <optional-maven-parameters>"
  echo "Valid test types are: performance, functional"
  exit 1
fi

DEPLOY_ID=$1
VPC=$2
SUBNETS=$3
RESULTS_BUCKET=$4
MAIN_SUITE_NAME=$5

shift 4
if [ "$MAIN_SUITE_NAME" == "performance" ]; then
  shift
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

echo "DEPLOY_ID=$DEPLOY_ID"
echo "MAIN_SUITE_NAME=$MAIN_SUITE_NAME"
echo "MAIN_SUITE_PARAMS=(${MAIN_SUITE_PARAMS[*]})"

source "$SCRIPTS_DIR/functions/timeUtils.sh"
source "$SCRIPTS_DIR/functions/systemTestUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
START_TIME_SHORT=$(recorded_time_str "$START_TIMESTAMP" "%m%d%H%M")
OUTPUT_DIR="$REPO_PARENT_DIR/logs/$START_TIME-$MAIN_SUITE_NAME"

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
    echo "Made output directory: $TEST_OUTPUT_DIR for SHORT_ID: $SHORT_ID"
    ./maven/deployTest.sh "$SHORT_ID" "$VPC" "$SUBNETS" \
      -Dsleeper.system.test.output.dir="$TEST_OUTPUT_DIR" \
      "${EXTRA_MAVEN_PARAMS[@]}" \
      &> "$OUTPUT_DIR/$TEST_NAME.log"
    RUN_TESTS_EXIT_CODE=$?
    echo "Exit code for $SHORT_ID is $RUN_TESTS_EXIT_CODE"
    if [ $RUN_TESTS_EXIT_CODE -ne 0 ]; then
      END_EXIT_CODE=$RUN_TESTS_EXIT_CODE
      TEST_EXIT_CODE=$RUN_TESTS_EXIT_CODE
    fi
    pushd "$MAVEN_DIR"
    echo "Running maven batch mode command for $SHORT_ID"
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
    echo "Short instance names=$SHORT_INSTANCE_NAMES"
    TEARDOWN_EXIT_CODE=$?
    if [ $TEARDOWN_EXIT_CODE -ne 0 ]; then
      TEST_EXIT_CODE=$TEARDOWN_EXIT_CODE
      END_EXIT_CODE=$TEARDOWN_EXIT_CODE
    fi
    echo -n "$TEST_EXIT_CODE $SHORT_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
}

pushd "$MAVEN_DIR"
    mvn clean
popd

if [ "$MAIN_SUITE_NAME" == "performance" ]; then
    echo "Running performance tests in parallel. Start time: [$(time_str)]"
    SUITE_PARAMS1=(-Dsleeper.system.test.cluster.enabled=true -DrunIT=ExpensiveSuite1 "$@")
    SUITE_PARAMS2=(-Dsleeper.system.test.cluster.enabled=true -DrunIT=ExpensiveSuite2 "$@")
    SUITE_PARAMS3=(-Dsleeper.system.test.cluster.enabled=true -DrunIT=ExpensiveSuite3 "$@")
    SUITE_PARAMS4=(-Dsleeper.system.test.cluster.enabled=true -DrunIT=ExpensiveSuite4 "$@")
    SUITE_PARAMS5=(-Dsleeper.system.test.cluster.enabled=true -DrunIT=ExpensiveSuite5 "$@")
    SUITE_PARAMS6=(-Dsleeper.system.test.cluster.enabled=true -DrunIT=ExpensiveSuite6 "$@")
    runMavenSystemTests "${DEPLOY_ID}${START_TIME_SHORT}1" "expensive1" "${SUITE_PARAMS1[@]}"&
    runMavenSystemTests "${DEPLOY_ID}${START_TIME_SHORT}2" "expensive2" "${SUITE_PARAMS2[@]}"
    # runMavenSystemTests "${DEPLOY_ID}mvn${START_TIME_SHORT}" "expensive3" "${SUITE_PARAMS3[@]}"&
    # runMavenSystemTests "${DEPLOY_ID}mvn${START_TIME_SHORT}" "expensive4" "${SUITE_PARAMS4[@]}"&
    # runMavenSystemTests "${DEPLOY_ID}mvn${START_TIME_SHORT}" "expensive5" "${SUITE_PARAMS5[@]}"&
    # runMavenSystemTests "${DEPLOY_ID}mvn${START_TIME_SHORT}" "expensive6" "${SUITE_PARAMS6[@]}"
    wait
else
    runMavenSystemTests "${DEPLOY_ID}mvn${START_TIME_SHORT}" $MAIN_SUITE_NAME "${MAIN_SUITE_PARAMS[@]}"
fi

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.nightly.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd

exit $END_EXIT_CODE
