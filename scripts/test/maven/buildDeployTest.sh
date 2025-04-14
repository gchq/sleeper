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

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <shortId> <vpc> <csv-list-of-subnets> <optional-maven-params>"
  exit 1
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

"$SCRIPTS_DIR/build/buildForTest.sh"
END_BUILD_TIME=$(record_time)

"$THIS_DIR/deployTest.sh" "$@" -pl system-test/system-test-suite
FINISH_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Finished"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Build finished at $(recorded_time_str "$END_BUILD_TIME"), took $(elapsed_time_str "$START_TIME" "$END_BUILD_TIME")"
echo "Deploy & test finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_BUILD_TIME" "$FINISH_TIME")"
echo "Overall, took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
