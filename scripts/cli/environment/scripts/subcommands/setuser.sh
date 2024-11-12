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
ENVIRONMENT_ID=$(cat "$ENVIRONMENTS_DIR/current.txt")
ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$ENVIRONMENT_ID"
OUTPUTS_FILE="$ENVIRONMENT_DIR/outputs.json"

if [ "$#" -lt 1 ]; then
  USERNAME=$(jq ".[\"$ENVIRONMENT_ID-SleeperEnvironment\"].BuildEC2LoginUser" "$OUTPUTS_FILE" --raw-output)
  echo "Setting default user: $USERNAME"
else
  USERNAME=$1
  echo "Setting user: $USERNAME"
fi

echo "$USERNAME" > "$ENVIRONMENTS_DIR/currentUser.txt"
