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

if [ "$#" -lt 1 ]; then
	echo "Usage: environment deploy <uniqueId> <optional_cdk_parameters>"
	exit 1
fi

ENVIRONMENT_ID=$1

if [ "$#" -lt 2 ]; then
	CDK_PARAMS=("--all")
else
  CDK_PARAMS=("$@")
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd .. && pwd)
CDK_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
ENVIRONMENTS_DIR=$(cd "$HOME/.sleeper/environments" && pwd)
ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$ENVIRONMENT_ID"
OUTPUTS_FILE="$ENVIRONMENT_DIR/outputs.json"

"$SCRIPTS_DIR/util/configure-aws.sh"

pushd "$CDK_DIR" > /dev/null
cdk deploy -c instanceId="$ENVIRONMENT_ID" --outputs-file "$OUTPUTS_FILE" "${CDK_PARAMS[@]}"
popd > /dev/null

echo "$ENVIRONMENT_ID" > "$ENVIRONMENTS_DIR/current.txt"

# If an EC2 was created, save SSH details
SSH_KEY_FILE="$ENVIRONMENT_ID-BuildEC2.pem"
if [ -f "$SSH_KEY_FILE" ]; then

  mv "$SSH_KEY_FILE" "$ENVIRONMENT_DIR/BuildEC2.pem"

  # Wait for deployment, scan SSH to remember EC2 certificate
  "$THIS_DIR/scan-ssh.sh"
fi
