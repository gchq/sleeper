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

#####################
# Initial variables #
#####################

if [[ -z $1 || -z $2 ]]; then
  echo "Usage: $0 <instance-id> <table name> <optional-delete-partitions-true-or-false> <optional-split-points-file-location> <optional-split-points-file-base64-encoded-true-or-false>"
  exit 1
fi

INSTANCE_ID=$1
TABLE_NAME=$2

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd "../" && pwd)
ARROW_FLAGS="--add-opens java.base/java.nio=ALL-UNNAMED"

if [[ -z $3 ]]; then
  java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar $ARROW_FLAGS sleeper.clients.status.update.ReinitialiseTable "${INSTANCE_ID}" "${TABLE_NAME}"
else
  DELETE_PARTITIONS=$3
  echo "Optional parameter for <delete-partitions> recognised and set to" "${DELETE_PARTITIONS}"
  if [[ -n $4 ]]; then
    SPLIT_POINT_FILE_LOCATION=$4
    echo "Optional parameter for <split-point-file-location> recognised and set to" "${SPLIT_POINT_FILE_LOCATION}"
    if [[ -n $5 ]]; then
      SPLIT_POINTS_FILE_ENCODED=$5
      echo "Optional parameter for <split-points-file-base64-encoded> recognised and set to" "${SPLIT_POINTS_FILE_ENCODED}"
      java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar $ARROW_FLAGS sleeper.clients.status.update.ReinitialiseTableFromSplitPoints "${INSTANCE_ID}" "${TABLE_NAME}" "${SPLIT_POINT_FILE_LOCATION}" "${SPLIT_POINTS_FILE_ENCODED}"
    else
      java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar $ARROW_FLAGS sleeper.clients.status.update.ReinitialiseTableFromSplitPoints "${INSTANCE_ID}" "${TABLE_NAME}" "${SPLIT_POINT_FILE_LOCATION}"
    fi
  else
    java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar $ARROW_FLAGS sleeper.clients.status.update.ReinitialiseTable "${INSTANCE_ID}" "${TABLE_NAME}" "${DELETE_PARTITIONS}"
  fi
fi
