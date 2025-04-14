#!/usr/bin/env bash
# Copyright 2022-2025 Crown Copyright
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

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ];  then
  echo "Usage: $0 <instance-id> <table-name> <optional-number-of-records>"
  exit 1
fi
INSTANCE_ID="$1"
TABLE_NAME="$2"
shift 2

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
OUTPUT_DIR="$THIS_DIR/output/"
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

mkdir -p "$OUTPUT_DIR"
java -cp "${SCRIPTS_DIR}/jars/system-test-data-generation-${VERSION}.jar" \
  sleeper.systemtest.datageneration.GenerateRandomDataFiles "$INSTANCE_ID" "$TABLE_NAME" "$OUTPUT_DIR" "$@"
