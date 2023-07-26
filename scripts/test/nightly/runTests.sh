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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
JAVA_DIR=$(cd "$SCRIPTS_DIR" && cd .. && pwd)

pushd "$SCRIPTS_DIR/test"

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <vpc> <subnet> <results bucket>"
  exit 1
fi

VPC=$1
SUBNETS=$2
RESULTS_BUCKET=$3

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
OUTPUT_DIR="/tmp/sleeper/performanceTests/$START_TIME"

mkdir -p "$OUTPUT_DIR"
../build/buildForTest.sh
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"
set +e

runReport() {
  INSTANCE_ID=$1
  shift 1
  for REPORT_TYPE in "$@" 
  do 
    case "$REPORT_TYPE" in
      "compaction")
        "$SCRIPTS_DIR/utility/compactionTaskStatusReport.sh" "$INSTANCE_ID" "standard" "-a"
        "$SCRIPTS_DIR/utility/compactionJobStatusReport.sh" "$INSTANCE_ID" "system-test" "standard" "-a"
        ;;
      "ingest")
        "$SCRIPTS_DIR/utility/ingestTaskStatusReport.sh" "$INSTANCE_ID" "standard" "-a"
        "$SCRIPTS_DIR/utility/ingestJobStatusReport.sh" "$INSTANCE_ID" "system-test" "standard" "-a"
        ;;
      "partition")
        "$SCRIPTS_DIR/utility/partitionsStatusReport.sh" "$INSTANCE_ID" "system-test"
        ;;
      *)
        echo "unknown report type: $REPORT_TYPE";
        ;;
    esac
  done;
}
runTest() {
  TEST_NAME=$1
  INSTANCE_ID=$2
  shift 2
  REPORT_TYPES=( "$@" )

  echo "[$(time_str)] Running $TEST_NAME test"
  "./$TEST_NAME/deployTest.sh" "$INSTANCE_ID" "$VPC" "$SUBNETS" &> "$OUTPUT_DIR/$TEST_NAME.log"
  EXIT_CODE=$?
  runReport "$INSTANCE_ID" "${REPORT_TYPES[@]}"  &> "$OUTPUT_DIR/$TEST_NAME.report.log"
  echo -n "$EXIT_CODE $INSTANCE_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
}

runSystemTest() {
    TEST_NAME=$1
    INSTANCE_ID=$2
    runTest "$@"
    ./tearDown.sh "$INSTANCE_ID" &> "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
}
runStandardTest() {
    TEST_NAME=$1
    INSTANCE_ID=$2
    runTest "$@"
    ./../deploy/tearDown.sh "$INSTANCE_ID" &> "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
}

runIngestBatcherTest() {
    TEST_NAME="ingestBatcher"
    INSTANCE_ID=$1
    pushd "$JAVA_DIR/system-test/system-test-suite"
    mvn -Dtest=IngestBatcherIT -PsystemTest \
      -Dsleeper.system.test.short.id="$INSTANCE_ID" \
      -Dsleeper.system.test.vpc.id="$VPC" \
      -Dsleeper.system.test.subnet.ids="$SUBNETS" verify &> "$OUTPUT_DIR/$TEST_NAME.log"
    popd
    runReport "$INSTANCE_ID" "ingest"  &> "$OUTPUT_DIR/$TEST_NAME.report.log"
}

runSystemTest bulkImportPerformance "bulk-imprt-$START_TIME" "ingest"
runSystemTest compactionPerformance "compaction-$START_TIME" "compaction" 
runSystemTest partitionSplitting "splitting-$START_TIME" "partition"
#runStandardTest ingestBatcher "ingst-batch-$START_TIME" "ingest"
runIngestBatcherTest "ingst-batcher" $START_TIME

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.nightly.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd
