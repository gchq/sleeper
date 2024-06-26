#!/usr/bin/env bash
# Copyright 2022-2024 Crown Copyright
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

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <instance-id> <vpc> <csv-list-of-subnets> <table-name>"
  exit 1
fi

INSTANCE_ID=$1
VPC=$2
SUBNET=$3
TABLE_NAME=$4

echo "-------------------------------------------------------------------------------"
echo "Running Build & Deploy"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "VPC: ${VPC}"
echo "SUBNET:${SUBNET}"
echo "TABLE_NAME: ${TABLE_NAME}"

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd ".." && pwd)
"$SCRIPTS_DIR/build/build.sh"
"$SCRIPTS_DIR/deploy/deployNew.sh" "$INSTANCE_ID" "$VPC" "$SUBNET" "$TABLE_NAME"
