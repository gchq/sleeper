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

this_dir=$(cd $(dirname $0) && pwd)
PROJECT_ROOT=$(dirname $(dirname ${this_dir}))
pushd ${PROJECT_ROOT}/java
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
DEPLOY_SCRIPTS_DIR="${PROJECT_ROOT}/scripts/deploy"
GENERATED_DIR="${PROJECT_ROOT}/scripts/generated"
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(fgrep sleeper.id ${INSTANCE_PROPERTIES} | cut -d'=' -f2)
CONFIG_BUCKET=$(cat ${GENERATED_DIR}/configBucket.txt)
TABLE_BUCKET=$(cat ${GENERATED_DIR}/tableBucket.txt)
QUERY_BUCKET=$(cat ${GENERATED_DIR}/queryResultsBucket.txt)
RETAIN_INFRA=`grep sleeper.retain.infra.after.destroy ${INSTANCE_PROPERTIES} | cut -d'=' -f2`

echo "RETAIN_INFRA:${RETAIN_INFRA}"
echo "PROJECT_ROOT:${PROJECT_ROOT}"
echo "GENERATED_DIR:${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES:${INSTANCE_PROPERTIES}"
echo "CONFIG_BUCKET:${CONFIG_BUCKET}"
echo "TABLE_BUCKET:${TABLE_BUCKET}"
echo "QUERY_BUCKET:${QUERY_BUCKET}"
echo "DEPLOY_SCRIPTS_DIR:${DEPLOY_SCRIPTS_DIR}"

pushd ${PROJECT_ROOT}

echo "Pausing the system"
java -cp java/clients/target/clients-*-utility.jar "sleeper.status.update.PauseSystem" ${INSTANCE_ID}

if [[ "${RETAIN_INFRA,,}" == "false" ]]; then
  echo "Removing all data from config, table and query results buckets"
  # Don't fail script if buckets don't exist
  aws s3 rm s3://${CONFIG_BUCKET} --recursive || true
  aws s3 rm s3://${TABLE_BUCKET} --recursive || true
  aws s3 rm s3://${QUERY_BUCKET} --recursive || true
fi

echo "Running cdk destroy to remove the system"
cdk -a "java -cp java/system-test/target/system-test-*-utility.jar sleeper.systemtest.cdk.SystemTestApp" \
destroy -c testpropertiesfile=${INSTANCE_PROPERTIES} "*"

echo "Removing the Jars bucket and docker containers"
${DEPLOY_SCRIPTS_DIR}/removeUploads.sh ${INSTANCE_PROPERTIES}
popd

echo "Removing generated files"
rm -r ${GENERATED_DIR}

echo "Successfully torn down"
