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

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <instance-id> <table-name> <optional-force-flag>"
  exit 1
fi

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)

TEMPLATE_DIR=${SCRIPTS_DIR}/templates
JAR_DIR=${SCRIPTS_DIR}/jars

VERSION=$(cat "${TEMPLATE_DIR}/version.txt")

echo "-------------------------------------------------------"
echo "Deleting table"
echo "-------------------------------------------------------"
java -cp "${JAR_DIR}/clients-${VERSION}-utility.jar" \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  sleeper.clients.status.update.DeleteTable "$@"
