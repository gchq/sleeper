#!/usr/bin/env bash
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

set -e
# Tears down a Sleeper instance

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd .. && pwd)
GENERATED_DIR=${SCRIPTS_DIR}/generated
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)
CONFIG_BUCKET=$(cat "${GENERATED_DIR}/configBucket.txt")
TABLE_BUCKET=$(cat "${GENERATED_DIR}/tableBucket.txt")
QUERY_BUCKET=$(cat "${GENERATED_DIR}/queryResultsBucket.txt")

echo "--------------------------------------------------------"
echo "Tear Down"
echo "--------------------------------------------------------"

echo "THIS_DIR: ${THIS_DIR}"
echo "SCRIPTS_DIR: ${SCRIPTS_DIR}"
echo "GENERATED_DIR: ${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES: ${INSTANCE_PROPERTIES}"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "CONFIG_BUCKET: ${CONFIG_BUCKET}"
echo "TABLE_BUCKET: ${TABLE_BUCKET}"
echo "QUERY_BUCKET: ${QUERY_BUCKET}"

echo "Pausing the system"
java -cp "${SCRIPTS_DIR}/jars/clients-*-utility.jar" "sleeper.status.update.PauseSystem" "${INSTANCE_ID}"

RETAIN_INFRA=$(grep sleeper.retain.infra.after.destroy "${INSTANCE_PROPERTIES}" | cut -d'=' -f2 | awk '{print tolower($0)}')
if [[ "${RETAIN_INFRA}" == "false" ]]; then
  echo "Removing all data from config, table and query results buckets"
  echo "Removing: ${CONFIG_BUCKET}"
  echo "Removing: ${TABLE_BUCKET}"
  echo "Removing: ${QUERY_BUCKET}"
  # Don't fail script if buckets don't exist
  aws s3 rm "s3://${CONFIG_BUCKET}" --recursive || true
  aws s3 rm "s3://${TABLE_BUCKET}" --recursive || true
  aws s3 rm "s3://${QUERY_BUCKET}" --recursive || true
fi

SLEEPER_VERSION=$(grep sleeper.version "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)

echo "Running cdk destroy to remove the system"
cdk -a "java -cp ${SCRIPTS_DIR}/jars/cdk-${SLEEPER_VERSION}.jar sleeper.cdk.SleeperCdkApp" \
destroy -c propertiesfile="${INSTANCE_PROPERTIES}" -c validate=false "*"

echo "Removing the Jars bucket and docker containers"
"${THIS_DIR}/removeUploads.sh" "${INSTANCE_PROPERTIES}"

echo "Removing generated files"
rm -r "${GENERATED_DIR:?}"/*

echo "Successfully torn down"
