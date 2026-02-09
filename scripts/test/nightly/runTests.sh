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
"$SCRIPTS_DIR/build/buildForTest.sh"
"$SCRIPTS_DIR/build/buildPython.sh"
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"

set +e

END_EXIT_CODE=0

docker buildx rm sleeper
set -e
docker buildx create --name sleeper --use
set +e

copyFolderForParallelRun() {
    COPY_DIR=$1
    echo "Making folder $COPY_DIR for parallel build"
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
    NEW_MAVEN_DIR=$(cd ../../java && pwd)
    SHORT_ID=$1
    TEST_NAME=$2
    shift 2
    TEST_EXIT_CODE=0
    EXTRA_MAVEN_PARAMS=("$@")
    TEST_OUTPUT_DIR="$OUTPUT_DIR/$TEST_NAME"
    mkdir "$TEST_OUTPUT_DIR"
    echo "Made output directory: $TEST_OUTPUT_DIR for SHORT_ID: $SHORT_ID"

    # Run tests
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

    # Generate site HTML
    pushd "$NEW_MAVEN_DIR"
    echo "Generating site HTML for $SHORT_ID"
    mvn --batch-mode site site:stage -pl system-test/system-test-suite \
       -DskipTests=true \
       -DstagingDirectory="$TEST_OUTPUT_DIR/site"
    popd
    pushd "$TEST_OUTPUT_DIR/site"
    zip -r "$OUTPUT_DIR/$TEST_NAME-site.zip" "."
    popd
    rm -rf "$TEST_OUTPUT_DIR/site"

    # Tear down instances used for tests
    SHORT_INSTANCE_NAMES=$(read_short_instance_names_from_instance_ids "$SHORT_ID" "$TEST_OUTPUT_DIR/instanceIds.txt")
    ./maven/tearDown.sh "$SHORT_ID" "$SHORT_INSTANCE_NAMES" &> "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
    TEARDOWN_EXIT_CODE=$?
    if [ $TEARDOWN_EXIT_CODE -ne 0 ]; then
      TEST_EXIT_CODE=$TEARDOWN_EXIT_CODE
      END_EXIT_CODE=$TEARDOWN_EXIT_CODE
    fi
    echo -n "$TEST_EXIT_CODE $SHORT_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
}

runTestSuite(){
    SUITE=$3
    copyFolderForParallelRun "$SUITE"
    # Wait short time to not have clashes with other process deploying
    sleep $1
    shift 1
    echo "[$(time_str)] Starting test suite: $SUITE"
    pushd "$REPO_PARENT_DIR/$SUITE/scripts/test" #Move into isolated repo copy
    runMavenSystemTests "$@"
    popd
    echo "[$(time_str)] Finished test suite: $SUITE"
    removeFolderAfterParallelRun "$SUITE"
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
