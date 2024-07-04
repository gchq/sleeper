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

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <comma-separated-short-ids> <optional-comma-separated-instance-short-names> <optional-comma-separated-standalone-instance-ids>"
  exit 1
fi

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd ../.. && pwd)
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

java -cp "${SCRIPTS_DIR}/jars/system-test-${VERSION}-utility.jar" \
  sleeper.systemtest.drivers.cdk.TearDownMavenSystemTest "${SCRIPTS_DIR}" "$@"
