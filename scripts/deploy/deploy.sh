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

if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <instance-id> <vpc> <csv-list-of-subnets> <instance-properties-file>[ options]"
  echo "Available options:"
  echo "--deploy-paused"
  echo "Deploy the instance with all scheduled rules disabled, so that background processes will not run."
  echo "The instance can be unpaused with 'scripts/utility/restartSystem.sh'."
  exit 1
fi

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar"  sleeper.clients.deploy.DeployInstance "${SCRIPTS_DIR}" "$@"
