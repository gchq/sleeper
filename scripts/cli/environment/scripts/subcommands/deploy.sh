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

if [ "$#" -lt 1 ]; then
  echo "Usage: environment deploy <unique-id> <optional-cdk-parameters>"
  exit 1
fi

ENVIRONMENT_ID=$1
shift

if [ "$#" -lt 1 ]; then
  CDK_PARAMS=()
else
  CDK_PARAMS=("$@")
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
CDK_ROOT_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
ENVIRONMENTS_DIR=$(cd "$HOME/.sleeper/environments" && pwd)
ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$ENVIRONMENT_ID"
OUTPUTS_FILE="$ENVIRONMENT_DIR/outputs.json"

pushd "$CDK_ROOT_DIR" > /dev/null
cdk deploy -c instanceId="$ENVIRONMENT_ID" --outputs-file "$OUTPUTS_FILE" --all "${CDK_PARAMS[@]}"
popd > /dev/null

USERNAME=$(jq ".[\"$ENVIRONMENT_ID-BuildEC2\"].LoginUser" "$OUTPUTS_FILE" --raw-output)

echo "$ENVIRONMENT_ID" > "$ENVIRONMENTS_DIR/current.txt"
echo "$USERNAME" > "$ENVIRONMENTS_DIR/currentUser.txt"

# If an EC2 was created, wait for deployment, make a test connection to remember SSH certificate
INSTANCE_ID=$(jq ".[\"$ENVIRONMENT_ID-BuildEC2\"].InstanceId" "$OUTPUTS_FILE" --raw-output)
if [ "$INSTANCE_ID" != "null" ]; then
  "$THIS_DIR/test-connection.sh"
fi
