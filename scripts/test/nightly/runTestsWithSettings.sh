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
  echo "Usage: $0 <settings-json-file> <test-type>"
  echo "Valid test types are: performance, functional"
  exit 1
fi

SETTINGS_FILE=$1
TEST_TYPE=$2

VPC=$(jq ".vpc" "$SETTINGS_FILE" --raw-output)
SUBNETS=$(jq ".subnets" "$SETTINGS_FILE" --raw-output)
RESULTS_BUCKET=$(jq ".resultsBucket" "$SETTINGS_FILE" --raw-output)
REPO_PATH=$(jq ".repoPath" "$SETTINGS_FILE" --raw-output)
MERGE_TO_MAIN=$(jq ".mergeToMainOnTestType.$TEST_TYPE" "$SETTINGS_FILE" --raw-output)

pushd "$THIS_DIR"

set +e
./runTests.sh "$VPC" "$SUBNETS" "$RESULTS_BUCKET" "$TEST_TYPE"
EXIT_CODE=$?
set -e

if [ "$MERGE_TO_MAIN" != "true" ]; then
  echo "Not merging changes into main branch due to settings for test type '$TEST_TYPE'"
elif [ $EXIT_CODE -ne 0 ]; then
  echo "Not merging changes into main branch because something failed, see logs for more information"
else
  echo "Will merge changes into main branch..."
  PRIVATE_KEY_FILE=$(jq ".gitHubApp.privateKeyFile" "$SETTINGS_FILE" --raw-output)
  APP_ID=$(jq ".gitHubApp.appId" "$SETTINGS_FILE" --raw-output)
  INSTALLATION_ID=$(jq ".gitHubApp.installationId" "$SETTINGS_FILE" --raw-output)
  # Copy merge to main script to keep the version from develop
  TMP_MERGE_TO_MAIN=$(mktemp)
  cat ./mergeToMain.sh >> "$TMP_MERGE_TO_MAIN"
  chmod u+x "$TMP_MERGE_TO_MAIN"
  pushd "$REPO_DIR"
  "$TMP_MERGE_TO_MAIN" "$REPO_PATH" "$PRIVATE_KEY_FILE" "$APP_ID" "$INSTALLATION_ID"
  popd
  rm "$TMP_MERGE_TO_MAIN"
fi

popd
