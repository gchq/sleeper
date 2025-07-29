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

TEST_USER=$1
TEST_TYPE=$2

# Builder mount directory is mounted into sleeper builder Docker container
BUILDER_MOUNT_HOST="/home/$TEST_USER/.sleeper/builder"
BUILDER_MOUNT_DOCKER="/sleeper-builder"

LOGS_DIR_HOST="$BUILDER_MOUNT_HOST/logs"
SETTINGS_FILE_DOCKER="$BUILDER_MOUNT_DOCKER/nightlyTestSettings.json"
SCRIPTS_DIR_DOCKER="$BUILDER_MOUNT_DOCKER/sleeper/scripts"

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
REMOVE_OLD_LOGS_LOG_HOST="$LOGS_DIR_HOST/$START_TIME-remove-old-logs.log"
DOCKER_PRUNE_LOG_HOST="$LOGS_DIR_HOST/$START_TIME-docker-prune.log"
CLI_UPGRADE_LOG_HOST="$LOGS_DIR_HOST/$START_TIME-cli-upgrade.log"
START_TESTS_LOG_HOST="$LOGS_DIR_HOST/$START_TIME-start-$TEST_TYPE-tests.log"

deleteOldLogs() {
    echo "Finding old logs to delete under $LOGS_DIR_HOST"
    find "$LOGS_DIR_HOST"/* -maxdepth 0 -type d -daystart -mtime +7 -exec echo "Deleting directory:" {} \; -exec rm -rf {} \;
    find "$LOGS_DIR_HOST"/* -maxdepth 0 -type f -daystart -mtime +7 -exec echo "Deleting file:" {} \; -exec rm -f {} \;
}

deleteOldLogs &> "$REMOVE_OLD_LOGS_LOG_HOST"
docker system prune -af &> "$DOCKER_PRUNE_LOG_HOST"
sleeper cli upgrade &> "$CLI_UPGRADE_LOG_HOST"
sleeper builder "$SCRIPTS_DIR_DOCKER/test/nightly/updateAndRunTests.sh" "$SETTINGS_FILE_DOCKER" "$TEST_TYPE" &> "$START_TESTS_LOG_HOST"
