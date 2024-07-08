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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_DIR=$(cd "$THIS_DIR" && cd ../../.. && pwd)

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <settings-json-file>"
  exit 1
fi

SETTINGS_FILE=$1

REPO_PATH=$(jq ".repoPath" "$SETTINGS_FILE" --raw-output)

pushd "$REPO_DIR"

git remote set-url origin "https://github.com/$REPO_PATH.git"
git fetch
git switch --discard-changes -C develop origin/develop

popd
