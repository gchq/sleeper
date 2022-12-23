#!/bin/bash
# Copyright 2022 Crown Copyright
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
# Tears down a Sleeper instance and associated System Testing framework

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
DEPLOY_SCRIPTS_DIR="${SCRIPTS_DIR}/deploy"
GENERATED_DIR="${SCRIPTS_DIR}/generated"
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)
CONFIG_BUCKET=$(cat "${GENERATED_DIR}/configBucket.txt")
TABLE_BUCKET=$(cat "${GENERATED_DIR}/tableBucket.txt")
QUERY_BUCKET=$(cat "${GENERATED_DIR}/queryResultsBucket.txt")
RETAIN_INFRA=$(grep sleeper.retain.infra.after.destroy "${INSTANCE_PROPERTIES}" | cut -d'=' -f2 | awk '{print tolower($0)}')

echo "RETAIN_INFRA:${RETAIN_INFRA}"
echo "SCRIPTS_DIR:${SCRIPTS_DIR}"
echo "GENERATED_DIR:${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES:${INSTANCE_PROPERTIES}"
echo "CONFIG_BUCKET:${CONFIG_BUCKET}"
echo "TABLE_BUCKET:${TABLE_BUCKET}"
echo "QUERY_BUCKET:${QUERY_BUCKET}"
echo "DEPLOY_SCRIPTS_DIR:${DEPLOY_SCRIPTS_DIR}"

source "${SCRIPTS_DIR}/functions/timeUtils.sh"
START_TIME=$(record_time)
echo "Started at $(recorded_time_str "$START_TIME")"

echo "Pausing the system"
java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" "sleeper.status.update.PauseSystem" "${INSTANCE_ID}"

END_PAUSE_TIME=$(record_time)
echo "Pause finished at $(recorded_time_str "$END_PAUSE_TIME"), took $(elapsed_time_str "$START_TIME" "$END_PAUSE_TIME")"

if [[ "${RETAIN_INFRA}" == "false" ]]; then
  echo "Removing all data from config, table and query results buckets"
  # Don't fail script if buckets don't exist
  aws s3 rm "s3://${CONFIG_BUCKET}" --recursive || true
  aws s3 rm "s3://${TABLE_BUCKET}" --recursive || true
  aws s3 rm "s3://${QUERY_BUCKET}" --recursive || true
fi

END_CLEAR_BUCKETS_TIME=$(record_time)
echo "Clear buckets finished at $(recorded_time_str "$END_CLEAR_BUCKETS_TIME"), took $(elapsed_time_str "$END_PAUSE_TIME" "$END_CLEAR_BUCKETS_TIME")"

echo "Running cdk destroy to remove the system"
cdk -a "java -cp ${SCRIPTS_DIR}/jars/system-test-${VERSION}-utility.jar sleeper.systemtest.cdk.SystemTestApp" \
destroy --force -c testpropertiesfile="${INSTANCE_PROPERTIES}" "*"

END_CDK_DESTROY_TIME=$(record_time)
echo "CDK destroy finished at $(recorded_time_str "$END_CDK_DESTROY_TIME"), took $(elapsed_time_str "$END_CLEAR_BUCKETS_TIME" "$END_CDK_DESTROY_TIME")"

echo "Removing the Jars bucket and docker containers"
"${DEPLOY_SCRIPTS_DIR}/removeUploads.sh" "${INSTANCE_PROPERTIES}"

echo "Removing generated files"
rm -r "${GENERATED_DIR:?}"/*

echo "Successfully torn down"

FINISH_TIME=$(record_time)
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Pause finished at $(recorded_time_str "$END_PAUSE_TIME"), took $(elapsed_time_str "$START_TIME" "$END_PAUSE_TIME")"
echo "Clear buckets finished at $(recorded_time_str "$END_CLEAR_BUCKETS_TIME"), took $(elapsed_time_str "$END_PAUSE_TIME" "$END_CLEAR_BUCKETS_TIME")"
echo "CDK destroy finished at $(recorded_time_str "$END_CDK_DESTROY_TIME"), took $(elapsed_time_str "$END_CLEAR_BUCKETS_TIME" "$END_CDK_DESTROY_TIME")"
echo "Removing buckets & files finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_CDK_DESTROY_TIME" "$FINISH_TIME")"
echo "Overall, took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
