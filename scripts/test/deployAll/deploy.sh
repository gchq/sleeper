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

# This script deploys a demonstration instance.
# If you want the system to be built first, use buildDeploy.sh.
# If you want test data to be added automatically, use deployTest.sh or buildDeployTest.sh.
# You can also use writeRandomData.sh to add test data after the fact.

set -e
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")
SYSTEM_TEST_JAR="${SCRIPTS_DIR}/jars/system-test-${VERSION}-utility.jar"

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

# Deploy the demonstration instance. With no --properties-file/--config-dir, the Java app falls back to the
# system test configuration in this directory, seeding it from the .template files here on first run.
java -cp "${SYSTEM_TEST_JAR}" sleeper.systemtest.drivers.cdk.DeployNewTestInstance "${SCRIPTS_DIR}" "$@"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished deployment"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Deploy finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
