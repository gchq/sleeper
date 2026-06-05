#!/usr/bin/env bash
# Copyright 2022-2026 Crown Copyright
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
REPO_DIR=$(cd "$SCRIPTS_DIR" && cd .. && pwd)
MAVEN_DIR=$(cd "$REPO_DIR" && cd java && pwd)
REPO_PARENT_DIR=$(cd "$REPO_DIR" && cd .. && pwd)

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
SUITE_PARAMS=("-Dsleeper.system.test.cluster.enabled=true" "-DskipRust" "-Dsleeper.system.test.create.multi.platform.builder=false")

shift 4
if [ "$MAIN_SUITE_NAME" == "performance" ] || [ "$MAIN_SUITE_NAME" == "functional" ]; then
  shift
elif [ "$1" == "--main" ]; then
  MAIN_SUITE_NAME=custom
  SUITE_PARAMS=("$2")
  shift 2
else
  MAIN_SUITE_NAME=custom
  SUITE_PARAMS=("$@")
fi

echo "DEPLOY_ID=$DEPLOY_ID"
echo "MAIN_SUITE_NAME=$MAIN_SUITE_NAME"
echo "SUITE_PARAMS=(${SUITE_PARAMS[*]})"

source "$SCRIPTS_DIR/functions/timeUtils.sh"
source "$SCRIPTS_DIR/functions/systemTestUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
START_TIME_SHORT=$(recorded_time_str "$START_TIMESTAMP" "%m%d%H%M")
OUTPUT_DIR="$REPO_PARENT_DIR/logs/$START_TIME-$MAIN_SUITE_NAME"

mkdir -p "$OUTPUT_DIR"
"$SCRIPTS_DIR/build/build.sh"
"$SCRIPTS_DIR/build/buildPython.sh"
"$SCRIPTS_DIR/dev/buildDockerImage.sh" base sleeper-base:current --multiplatform
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"

set +e

END_EXIT_CODE=0

docker buildx rm sleeper
set -e
docker buildx create --name sleeper --use
set +e

copyFolderForParallelRun() {
    local COPY_DIR=$1
    echo "Making parallel build folder: $COPY_DIR"
    pushd $REPO_PARENT_DIR
    sudo rm -rf $COPY_DIR
    mkdir $COPY_DIR
    sudo rsync -a --exclude=".*" sleeper/ $COPY_DIR
    # A Python virtual environment includes an absolute path reference to itself, so update it
    sudo "$SCRIPTS_DIR/functions/run/sedInPlace.sh" \
      -e "s|sleeper/python|$COPY_DIR/python|" \
      "$COPY_DIR/python/env/bin/activate"
    popd
}

removeFolderAfterParallelRun() {
    echo "Removing folder $1"
    pushd $REPO_PARENT_DIR
    sudo rm -rf $1
    popd
}

runMavenSystemTests() {
    # Setup
    local NEW_MAVEN_DIR=$(cd ../../java && pwd)
    local SHORT_ID=$1
    local TEST_NAME=$2
    shift 2
    local TEST_EXIT_CODE=0
    local EXTRA_MAVEN_PARAMS=("$@")
    local TEST_OUTPUT_DIR="$OUTPUT_DIR/$TEST_NAME"
    mkdir "$TEST_OUTPUT_DIR"
    echo "Made output directory: $TEST_OUTPUT_DIR for SHORT_ID: $SHORT_ID"

    # Run tests
    local TEST_START=$(record_time)
    echo "[$(recorded_time_str "$TEST_START")] Running Maven test suite with short ID: $SHORT_ID"
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
    local TEST_END=$(record_time)

    # Generate site HTML
    pushd "$NEW_MAVEN_DIR"
    echo "[$(recorded_time_str "$TEST_END")] Generating site HTML for short ID: $SHORT_ID"
    mvn --batch-mode site site:stage -pl system-test/system-test-suite \
       -DskipTests=true \
       -DstagingDirectory="$TEST_OUTPUT_DIR/site"
    popd
    pushd "$TEST_OUTPUT_DIR/site"
    zip -r "$OUTPUT_DIR/$TEST_NAME-site.zip" "."
    popd
    rm -rf "$TEST_OUTPUT_DIR/site"
    local SITE_END=$(record_time)

    # Tear down instances used for tests
    local SHORT_INSTANCE_NAMES=$(read_short_instance_names_from_instance_ids "$SHORT_ID" "$TEST_OUTPUT_DIR/instanceIds.txt")
    ./maven/tearDown.sh "$SHORT_ID" "$SHORT_INSTANCE_NAMES" &> "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
    local TEARDOWN_EXIT_CODE=$?
    if [ $TEARDOWN_EXIT_CODE -ne 0 ]; then
      TEST_EXIT_CODE=$TEARDOWN_EXIT_CODE
      END_EXIT_CODE=$TEARDOWN_EXIT_CODE
    fi
    echo -n "$TEST_EXIT_CODE $SHORT_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
    local TEARDOWN_END=$(record_time)
    echo "[$(recorded_time_str "$SUITE_END")] Finished teardown of short ID: $SHORT_ID"
    echo "Started at $(recorded_time_str "$TEST_START")"
    echo "Finished Maven test suite at $(recorded_time_str "$TEST_END"), took $(elapsed_time_str "$TEST_START" "$TEST_END")"
    echo "Finished site HTML at $(recorded_time_str "$SITE_END"), took $(elapsed_time_str "$TEST_END" "$SITE_END")"
    echo "Finished tear down at $(recorded_time_str "$TEARDOWN_END"), took $(elapsed_time_str "$SITE_END" "$TEARDOWN_END")"
}

runTestSuite(){
    local SUITE=$3
    local COPY_START=$(record_time)
    copyFolderForParallelRun "$SUITE"
    local COPY_END=$(record_time)
    echo "[$(recorded_time_str "$COPY_END")] Finished folder copy for suite: $SUITE, took $(elapsed_time_str "$COPY_START" "$COPY_END")"
    # Wait short time to not have clashes with other process deploying
    sleep $1
    shift 1
    local SUITE_START=$(record_time)
    echo "[$(recorded_time_str "$SUITE_START")] Starting test suite: $SUITE"
    pushd "$REPO_PARENT_DIR/$SUITE/scripts/test" #Move into isolated repo copy
    runMavenSystemTests "$@"
    popd
    local SUITE_END=$(record_time)
    echo "[$(recorded_time_str "$SUITE_END")] Removing folder for test suite: $SUITE"
    removeFolderAfterParallelRun "$SUITE"
    local REMOVE_FOLDER_END=$(record_time)
    echo "[$(recorded_time_str "$REMOVE_FOLDER_END")] Finished test suite: $SUITE"
    echo "Folder copy ran from $(recorded_time_str "$COPY_START") to $(recorded_time_str "$COPY_END"), took $(elapsed_time_str "$COPY_START" "$COPY_END")"
    echo "Maven operations and tear down ran from $(recorded_time_str "$SUITE_START") to $(recorded_time_str "$SUITE_END"), took $(elapsed_time_str "$SUITE_START" "$SUITE_END")"
    echo "Folder removal finished at $(recorded_time_str "$REMOVE_FOLDER_END"), took $(elapsed_time_str "$SUITE_END" "$REMOVE_FOLDER_END")"
}

if [ "$MAIN_SUITE_NAME" == "performance" ]; then
    echo "Running performance tests in parallel. Start time: [$(time_str)]"
    runTestSuite 0 "${DEPLOY_ID}${START_TIME_SHORT}s1" "slow1" "${SUITE_PARAMS[@]}" "-DrunIT=SlowSystemTestSuite1" "$@" &> "$OUTPUT_DIR/slow1.suite.log" &
    runTestSuite 300 "${DEPLOY_ID}${START_TIME_SHORT}s2" "slow2" "${SUITE_PARAMS[@]}" "-DrunIT=SlowSystemTestSuite2" "$@" &> "$OUTPUT_DIR/slow2.suite.log" &
    runTestSuite 600 "${DEPLOY_ID}${START_TIME_SHORT}s3" "slow3" "${SUITE_PARAMS[@]}" "-DrunIT=SlowSystemTestSuite3" "$@" &> "$OUTPUT_DIR/slow3.suite.log" &
    runTestSuite 900 "${DEPLOY_ID}${START_TIME_SHORT}e1" "expensive1" "${SUITE_PARAMS[@]}" "-DrunIT=ExpensiveSystemTestSuite1" "$@" &> "$OUTPUT_DIR/expensive1.suite.log"  &
    runTestSuite 1200 "${DEPLOY_ID}${START_TIME_SHORT}e2" "expensive2" "${SUITE_PARAMS[@]}" "-DrunIT=ExpensiveSystemTestSuite2" "$@" &> "$OUTPUT_DIR/expensive2.suite.log"  &
    runTestSuite 1500 "${DEPLOY_ID}${START_TIME_SHORT}e3" "expensive3" "${SUITE_PARAMS[@]}" "-DrunIT=ExpensiveSystemTestSuite3" "$@" &> "$OUTPUT_DIR/expensive3.suite.log"  &
    runTestSuite 1800 "${DEPLOY_ID}${START_TIME_SHORT}q1" "quick" "${SUITE_PARAMS[@]}" "-DrunIT=QuickSystemTestSuite" "$@" &> "$OUTPUT_DIR/quick.suite.log" &
    wait
elif [ "$MAIN_SUITE_NAME" == "functional" ]; then
    echo "Running slow tests in parallel. Start time: [$(time_str)]"
    runTestSuite 0 "${DEPLOY_ID}${START_TIME_SHORT}s1" "slow1" "${SUITE_PARAMS[@]}" "-DrunIT=SlowSystemTestSuite1" "$@" &> "$OUTPUT_DIR/slow1.suite.log" &
    runTestSuite 300 "${DEPLOY_ID}${START_TIME_SHORT}s2" "slow2" "${SUITE_PARAMS[@]}" "-DrunIT=SlowSystemTestSuite2" "$@" &> "$OUTPUT_DIR/slow2.suite.log" &
    runTestSuite 600 "${DEPLOY_ID}${START_TIME_SHORT}s3" "slow3" "${SUITE_PARAMS[@]}" "-DrunIT=SlowSystemTestSuite3" "$@" &> "$OUTPUT_DIR/slow3.suite.log" &
    runTestSuite 900 "${DEPLOY_ID}${START_TIME_SHORT}q1" "quick" "${SUITE_PARAMS[@]}" "-DrunIT=QuickSystemTestSuite" "$@" &> "$OUTPUT_DIR/quick.suite.log" &
    wait
else
    runMavenSystemTests "${DEPLOY_ID}mvn${START_TIME_SHORT}" $MAIN_SUITE_NAME "${SUITE_PARAMS[@]}"
fi

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.nightly.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd

exit $END_EXIT_CODE
