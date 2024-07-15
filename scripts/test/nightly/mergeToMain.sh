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

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <repo-path> <private-key-pem-file> <app-id> <installation-id>"
  exit 1
fi

REPO_PATH=$1
PRIVATE_KEY=$2
APP_ID=$3
INSTALLATION_ID=$4

pushd java
echo "Compiling build module..."
mvn compile -Pquick -q -pl build -am

pushd build
echo "Generating access token..."
ACCESS_TOKEN_FILE=$(mktemp)
mvn exec:java -q \
  -Dexec.mainClass="sleeper.build.github.app.GenerateGitHubAppInstallationAccessToken" \
  -Dexec.args="$PRIVATE_KEY $APP_ID $INSTALLATION_ID $ACCESS_TOKEN_FILE"

ACCESS_TOKEN=$(<"$ACCESS_TOKEN_FILE")
rm "$ACCESS_TOKEN_FILE"
popd
popd

git remote set-url origin "https://x-access-token:$ACCESS_TOKEN@github.com/$REPO_PATH.git"
git switch --discard-changes -C main origin/main
git pull
git merge develop
git push
git remote set-url origin "https://github.com/$REPO_PATH.git"
git fetch
git switch --discard-changes -C develop origin/develop
