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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

java -cp "${SCRIPTS_DIR}/jars/system-test-${VERSION}-utility.jar" \
  sleeper.systemtest.ingest.batcher.SystemTestForIngestBatcher \
  "${SCRIPTS_DIR}" "$THIS_DIR/system-test-instance.properties" "$@"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished paused deploy & test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Tests finished at $(recorded_time_str "$FINISH_TIME")"
echo "Overall, deploy & paused test took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
