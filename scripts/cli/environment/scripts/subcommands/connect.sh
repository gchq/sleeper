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

ENVIRONMENTS_DIR=$(cd "$HOME/.sleeper/environments" && pwd)

if [ "$#" -gt 0 ]; then
  SSH_PARAMS=("$@")
else
  SSH_PARAMS=(screen -d -RR)
fi

echo "SSH_PARAMS: " "${SSH_PARAMS[@]}"

ENVIRONMENT_ID=$(cat "$ENVIRONMENTS_DIR/current.txt")
USERNAME=$(cat "$ENVIRONMENTS_DIR/currentUser.txt")

ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$ENVIRONMENT_ID"
OUTPUTS_FILE="$ENVIRONMENT_DIR/outputs.json"
KNOWN_HOSTS_FILE="$ENVIRONMENT_DIR/known_hosts"

INSTANCE_ID=$(jq ".[\"$ENVIRONMENT_ID-SleeperEnvironment\"].BuildEC2Id" "$OUTPUTS_FILE" --raw-output)
TEMP_KEY_DIR=$(mktemp -d)
TEMP_KEY_PATH="$TEMP_KEY_DIR/sleeper-environment-connect-key"

print_time() {
  date -u +"%T UTC"
}

# Use EC2 Instance Connect to create an SSH key for the user we want to connect as
echo "[$(print_time)] Generating temporary SSH key..."
ssh-keygen -q -t rsa -N '' -f "$TEMP_KEY_PATH"
echo "[$(print_time)] Uploading public key..."
aws ec2-instance-connect send-ssh-public-key \
  --instance-id "$INSTANCE_ID" \
  --instance-os-user "$USERNAME" \
  --ssh-public-key "file://$TEMP_KEY_PATH.pub"

# Use SSM Session Manager to tunnel to the EC2 for SSH
echo "[$(print_time)] Connecting..."
ssh -o "UserKnownHostsFile=$KNOWN_HOSTS_FILE" -o "IdentitiesOnly=yes" -i "$TEMP_KEY_PATH" -t \
  -o "ProxyCommand=sh -c \"aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters 'portNumber=%p'\"" \
  "$USERNAME@$INSTANCE_ID" "${SSH_PARAMS[@]}"

rm -f "$TEMP_KEY_DIR"/*
rmdir "$TEMP_KEY_DIR"
