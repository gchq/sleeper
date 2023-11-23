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

pushd "$THIS_DIR"

git fetch
git switch --discard-changes -C develop origin/develop

popd

set +e
"$THIS_DIR/runTests.sh" "$@"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  git switch --discard-changes -C main origin/main
  git merge origin/develop
  git push
fi
