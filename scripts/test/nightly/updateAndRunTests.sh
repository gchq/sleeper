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
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
MAVEN_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)

if [ "$#" -ne 8 ]; then
  echo "Usage: $0 <vpc> <subnet> <results-bucket> <test-type> <repo-path> <private-key-pem-file> <app-id> <installation-id>"
  echo "Valid test types are: performance, functional"
  echo "Private key, app ID and installation ID are for authenticating as a GitHub App to push to main"
  exit 1
fi

VPC=$1
SUBNETS=$2
RESULTS_BUCKET=$3
TEST_TYPE=$4
REPO_PATH=$5
PRIVATE_KEY=$6
APP_ID=$7
INSTALLATION_ID=$8

pushd "$THIS_DIR"

git remote set-url origin "https://github.com/$REPO_PATH.git"
git fetch
git switch --discard-changes -C develop origin/develop

popd

set +e
"$THIS_DIR/runTests.sh" "$VPC" "$SUBNETS" "$RESULTS_BUCKET" "$TEST_TYPE"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then

  pushd "$MAVEN_DIR"
  echo "Compiling build module..."
  mvn compile -Pquick -q -pl build -am

  pushd build
  echo "Generating access token..."
  ACCESS_TOKEN_FILE=$(mktemp)
  mvn exec:java -q \
    -Dexec.mainClass="sleeper.build.github.app.GenerateGitHubAppInstallationAccessToken" \
    -Dexec.args="$PRIVATE_KEY $APP_ID $INSTALLATION_ID $ACCESS_TOKEN_FILE"
  popd

  popd

  ACCESS_TOKEN=$(<"$ACCESS_TOKEN_FILE")
  git remote set-url origin "https://x-access-token:$ACCESS_TOKEN@github.com/$REPO_PATH.git"
  git switch --discard-changes -C main origin/main
  git pull
  git merge develop
  git push
fi
