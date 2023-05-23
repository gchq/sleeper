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
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd .. && pwd)

pushd "$THIS_DIR"

git fetch
git switch --discard-changes -C main origin/main

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <vpc> <subnet> <results bucket>"
  exit 1
fi

VPC=$1
SUBNET=$2
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

runTest() {
  TEST_NAME=$1
  INSTANCE_ID=$2

  echo "[$(time_str)] Running $TEST_NAME test"
  "./$TEST_NAME/deployTest.sh" "$INSTANCE_ID" "$VPC" "$SUBNET" &> "$OUTPUT_DIR/$TEST_NAME.log"
  echo -n "$? $INSTANCE_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
}

echo "[$(time_str) Cleaning up old test instances"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.nightly.cleanup.CleanupNightlyTestInstances "$RESULTS_BUCKET"

runTest bulkImportPerformance "bulk-imprt-$START_TIME"
runTest compactionPerformance "compaction-$START_TIME"
runTest partitionSplitting "splitting-$START_TIME"

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.nightly.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd
