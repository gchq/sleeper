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

ENVIRONMENTS_DIR=$(cd "$HOME/.sleeper/environments" && pwd)

if [ "$#" -gt 0 ]; then
  SSH_PARAMS=("$@")
else
  SSH_PARAMS=(screen -d -RR)
fi

ENVIRONMENT_ID=$(cat "$ENVIRONMENTS_DIR/current.txt")

ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$ENVIRONMENT_ID"
OUTPUTS_FILE="$ENVIRONMENT_DIR/outputs.json"
KNOWN_HOSTS_FILE="$ENVIRONMENT_DIR/known_hosts"
PRIVATE_KEY_FILE="$ENVIRONMENT_DIR/BuildEC2.pem"

USER=$(jq ".[\"$ENVIRONMENT_ID-BuildEC2\"].LoginUser" "$OUTPUTS_FILE" --raw-output)
EC2_IP=$(jq ".[\"$ENVIRONMENT_ID-BuildEC2\"].PublicIP" "$OUTPUTS_FILE" --raw-output)

ssh -i "$PRIVATE_KEY_FILE" -o "UserKnownHostsFile=$KNOWN_HOSTS_FILE" -t "$USER@$EC2_IP" "${SSH_PARAMS[@]}"
