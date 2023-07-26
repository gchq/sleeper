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

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <uniqueId> <vpc> <subnet>"
  exit 1
fi

INSTANCE_ID=$1
VPC=$2
SUBNETS=$3

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
JAVA_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

pushd "$JAVA_DIR/system-test/system-test-suite"
mvn -Dtest=IngestBatcherIT -PsystemTest \
  -Dsleeper.system.test.short.id="$INSTANCE_ID" \
  -Dsleeper.system.test.vpc.id="$VPC" \
  -Dsleeper.system.test.subnet.ids="$SUBNETS" verify
popd

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished paused deploy & test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Tests finished at $(recorded_time_str "$FINISH_TIME")"
echo "Overall, deploy & paused test took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
