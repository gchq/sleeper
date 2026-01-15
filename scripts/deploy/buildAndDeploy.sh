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

if [ "$#" -lt 3 ] || [ "$#" -gt 5 ]; then
  echo "Usage: $0 <instance-id> <vpc> <csv-list-of-subnets> <optional-instance-properties-file> <optional-deploy-paused-flag>"
  exit 1
fi

INSTANCE_ID=$1
VPC=$2
SUBNET=$3

INSTANCE_PROPERTIES="${4:-$INSTANCE_PROPERTIES}"
DEPLOY_PAUSED="${5:-$DEPLOY_PAUSED}"


echo "-------------------------------------------------------------------------------"
echo "Running Build & Deploy"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "VPC: ${VPC}"
echo "SUBNET:${SUBNET}"
echo "INSTANCE_PROPERTIES: ${INSTANCE_PROPERTIES}"
echo "DEPLOY_PAUSED: ${DEPLOY_PAUSED}"

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd ".." && pwd)
"$SCRIPTS_DIR/build/build.sh"
"$SCRIPTS_DIR/deploy/deployNew.sh" "$@"
