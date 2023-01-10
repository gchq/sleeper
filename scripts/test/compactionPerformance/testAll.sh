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

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <instanceId>"
	exit 1
fi

INSTANCE_ID=$1

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

"$THIS_DIR/testIngest.sh" "$INSTANCE_ID"

END_INGEST=$(record_time)
echo "Ingest finished at $(recorded_time_str "$END_INGEST"), took $(elapsed_time_str "$END_PAUSE_SYSTEM" "$END_INGEST")"

"$THIS_DIR/testCompaction.sh"

END_COMPACTION=$(record_time)
echo "Compaction finished at $(recorded_time_str "$END_COMPACTION"), took $(elapsed_time_str "$END_INGEST" "$END_COMPACTION")"

"$THIS_DIR/testSplittingCompaction.sh"

END_COMPACTION=$(record_time)
echo "Compaction finished at $(recorded_time_str "$END_COMPACTION"), took $(elapsed_time_str "$END_INGEST" "$END_COMPACTION")"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished paused test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Deploy finished at $(recorded_time_str "$END_DEPLOY_TIME"), took $(elapsed_time_str "$START_TIME" "$END_DEPLOY_TIME")"
echo "Starting of tasks to write random data finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_DEPLOY_TIME" "$FINISH_TIME")"
echo "Overall, deploying paused test took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
