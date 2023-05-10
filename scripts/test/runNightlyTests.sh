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

git fetch
git switch --discard-changes -C 806-automate-performance-tests-in-ec2 origin/806-automate-performance-tests-in-ec2

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <vpc> <subnet> <results bucket>"
  exit 1
fi

VPC=$1
SUBNET=$2
RESULTS_BUCKET=$3

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd .. && pwd)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
OUTPUT_DIR="/tmp/sleeper/performanceTests/$START_TIME"

pushd "$THIS_DIR"

mkdir -p "$OUTPUT_DIR"
../build/buildForTest.sh
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"
set +e

echo "[$(time_str)] Running bulkImportPerformance test"
./bulkImportPerformance/deployTest.sh "bi-perf-$START_TIME" "$VPC" "$SUBNET" &> "$OUTPUT_DIR/bulkImportPerformance.log"
echo -n "$?" > "$OUTPUT_DIR/bulkImportPerformance.status"

echo "[$(time_str)] Running compactionPerformance test"
./compactionPerformance/deployTest.sh "compn-perf-$START_TIME" "$VPC" "$SUBNET" &> "$OUTPUT_DIR/compactionPerformance.log"
echo -n "$?" > "$OUTPUT_DIR/compactionPerformance.status"

echo "[$(time_str)] Running partitionSplitting test"
./partitionSplitting/deployTest.sh "partn-splt-$START_TIME" "$VPC" "$SUBNET" &> "$OUTPUT_DIR/partitionSplitting.log"
echo -n "$?" > "$OUTPUT_DIR/partitionSplitting.status"

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.output.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd
