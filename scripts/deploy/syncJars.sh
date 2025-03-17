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

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <bucket-name> <region> <optional-delete-old-jars>"
  exit 1
fi

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
JARS_DIR="${SCRIPTS_DIR}/jars"
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

java -cp "${JARS_DIR}/clients-${VERSION}-utility.jar" sleeper.clients.deploy.SyncJars "${JARS_DIR}" "$@"
