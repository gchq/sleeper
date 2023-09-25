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
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
ENVIRONMENTS_DIR=$(cd "$HOME/.sleeper/environments" && pwd)

list_environments() {
  "$THIS_DIR/list.sh"
}

if [ "$#" -lt 1 ]; then
  echo "Usage: environment set <unique-id>"
  list_environments
  exit 1
fi

ENVIRONMENT_ID=$1

if [ -d "$ENVIRONMENTS_DIR/$ENVIRONMENT_ID" ]; then
  echo "$ENVIRONMENT_ID" > "$ENVIRONMENTS_DIR/current.txt"
else
  echo "Environment not found: $ENVIRONMENT_ID"
  list_environments
  exit 1
fi
