#!/usr/bin/env bash
#
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
#

set -e
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

if [ "$#" -ne 8 ]; then
  echo "Usage: $0 <settings-json-file> <test-type>"
  echo "Valid test types are: performance, functional"
  echo "Private key, app ID and installation ID are for authenticating as a GitHub App to push to main"
  exit 1
fi

SETTINGS_FILE=$1
TEST_TYPE=$2

VPC=$(jq ".vpc" "$SETTINGS_FILE" --raw-output)
SUBNETS=$(jq ".subnets" "$SETTINGS_FILE" --raw-output)
RESULTS_BUCKET=$(jq ".resultsBucket" "$SETTINGS_FILE" --raw-output)
REPO_PATH=$(jq ".repoPath" "$SETTINGS_FILE" --raw-output)
PRIVATE_KEY_FILE=$(jq ".gitHubApp.privateKeyFile" "$SETTINGS_FILE" --raw-output)
APP_ID=$(jq ".gitHubApp.appId" "$SETTINGS_FILE" --raw-output)
INSTALLATION_ID=$(jq ".gitHubApp.installationId" "$SETTINGS_FILE" --raw-output)

pushd "$THIS_DIR"

git remote set-url origin "https://github.com/$REPO_PATH.git"
git fetch
git switch --discard-changes -C develop origin/develop

set +e
./runTests.sh "$VPC" "$SUBNETS" "$RESULTS_BUCKET" "$TEST_TYPE"
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -eq 0 ]; then
  ./mergeToMain.sh "$REPO_PATH" "$PRIVATE_KEY_FILE" "$APP_ID" "$INSTALLATION_ID"
fi

popd
