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

# This script deploys a demonstration instance and adds some test data automatically.
# If you want the system to be built first, use buildDeployTest.sh.
# If you don't want test data to be added automatically, use deploy.sh or buildDeploy.sh.

set -e
unset CDPATH

if [ "$#" -lt 3 ] || [ "$#" -gt 5 ]; then
  echo "Usage: $0 <instance-id> <vpc> <csv-list-of-subnets> <optional-deploy-paused-flag> <optional-split-points-file>"
  exit 1
fi

INSTANCE_ID=$1

TABLE_NAME="system-test"
THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

"$THIS_DIR/deploy.sh" "$@"
END_DEPLOY_TIME=$(record_time)

"$THIS_DIR/writeRandomData.sh" "$INSTANCE_ID" "$TABLE_NAME"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished deploying test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Deploy finished at $(recorded_time_str "$END_DEPLOY_TIME"), took $(elapsed_time_str "$START_TIME" "$END_DEPLOY_TIME")"
echo "Starting of tasks to write random data finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_DEPLOY_TIME" "$FINISH_TIME")"
echo "Overall, deploying test took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
