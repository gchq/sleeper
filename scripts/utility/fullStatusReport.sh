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

#####################
# Initial variables #
#####################

if [[ -z $1 || -z $2 ]]; then
  echo "Usage: $0 <instance-id> <table-name>"
  exit 1
fi

INSTANCE_ID=$1
TABLE_NAME=$2

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd "../" && pwd)

if [[ -z $3 ]]; then
  java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    sleeper.clients.status.report.StatusReport "${INSTANCE_ID}" "${TABLE_NAME}"
else
  VERBOSE=$3
  echo "Optional Parameter for <Verbose> recognised and set to" "${VERBOSE}"
  java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    sleeper.clients.status.report.StatusReport "${INSTANCE_ID}" "${TABLE_NAME}" "${VERBOSE}"
fi
