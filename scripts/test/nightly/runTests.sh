#!/usr/bin/env bash
#
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
#

set -e
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
MAVEN_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)

pushd "$SCRIPTS_DIR/test"

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <vpc> <subnet> <results-bucket> <test-type>"
  echo "Valid test types are: performance, functional"
  exit 1
fi

VPC=$1
SUBNETS=$2
RESULTS_BUCKET=$3
if [ "$4" == "performance" ]; then
  TEST_SUITE_PARAMS="-Dsleeper.system.test.cluster.enabled=true"
  TEST_SUITE_NAME="performance"
elif [ "$4" == "functional" ]; then
  TEST_SUITE_PARAMS="-Dsleeper.system.test.cluster.enabled=false"
  TEST_SUITE_NAME="functional"
else
  echo "Invalid test type: $4"
  echo "Valid test types are: performance, functional"
  exit 1
fi
source "$SCRIPTS_DIR/functions/timeUtils.sh"
source "$SCRIPTS_DIR/functions/systemTestUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
OUTPUT_DIR="/tmp/sleeper/${TEST_SUITE_NAME}Tests/$START_TIME"

mkdir -p "$OUTPUT_DIR"
../build/buildForTest.sh
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"
set +e

runMavenSystemTests() {
    SHORT_ID=$1
    TEST_NAME=$2
    EXTRA_MAVEN_PARAMS=$3
    mkdir "$OUTPUT_DIR/$TEST_NAME"
    ./maven/deployTest.sh "$SHORT_ID" "$VPC" "$SUBNETS" \
      -Dsleeper.system.test.output.dir="$OUTPUT_DIR/$TEST_NAME" \
      "$EXTRA_MAVEN_PARAMS" \
      &> "$OUTPUT_DIR/$TEST_NAME.log"
    EXIT_CODE=$?
    echo -n "$EXIT_CODE $SHORT_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
    pushd "$MAVEN_DIR"
    mvn --batch-mode site site:stage -pl system-test/system-test-suite \
       -DskipTests=true \
       -DstagingDirectory="$OUTPUT_DIR/site"
    popd
    pushd "$OUTPUT_DIR/site"
    zip -r "../site.zip" "."
    popd
    rm -rf "$OUTPUT_DIR/site"
    INSTANCE_IDS=()
    read_instance_ids_to_array "$OUTPUT_DIR/$TEST_NAME/instanceIds.txt" INSTANCE_IDS
    ./maven/tearDown.sh "$SHORT_ID" "${INSTANCE_IDS[@]}" &> "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
}

runMavenSystemTests "mvn-$START_TIME" $TEST_SUITE_NAME $TEST_SUITE_PARAMS
runMavenSystemTests "s3-$START_TIME" s3-state-store -Dsleeper.system.test.force.statestore.classname=sleeper.statestore.s3.S3StateStore

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.nightly.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd
