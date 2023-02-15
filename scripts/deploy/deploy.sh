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

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <uniqueId> <vpc> <subnet> <table-name>"
  exit 1
fi

INSTANCE_ID=$1
VPC=$2
SUBNET=$3
TABLE_NAME=$4

echo "-------------------------------------------------------------------------------"
echo "Running Deployment"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "VPC: ${VPC}"
echo "SUBNET:${SUBNET}"
echo "TABLE_NAME: ${TABLE_NAME}"

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)

TEMPLATE_DIR=${SCRIPTS_DIR}/templates
GENERATED_DIR=${SCRIPTS_DIR}/generated
INSTANCE_PROPERTIES_PATH=${GENERATED_DIR}/instance.properties
JAR_DIR=${SCRIPTS_DIR}/jars


echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"
echo "GENERATED_DIR:${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES: ${INSTANCE_PROPERTIES_PATH}"
echo "SCRIPTS_DIR: ${SCRIPTS_DIR}"
echo "JAR_DIR: ${JAR_DIR}"

VERSION=$(cat "${TEMPLATE_DIR}/version.txt")
echo "VERSION: ${VERSION}"

mkdir -p "${GENERATED_DIR}"

echo "Copying instance properties template into generated files for use in pre-deployment steps"
cp "${TEMPLATE_DIR}/instanceproperties.template" "${INSTANCE_PROPERTIES_PATH}"

echo "Running Pre-deployment steps"
java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" sleeper.clients.admin.deploy.PreDeployInstance "${SCRIPTS_DIR}" "${INSTANCE_ID}" "${VPC}" "${SUBNET}" "${TABLE_NAME}"

echo "-------------------------------------------------------"
echo "Deploying Stacks"
echo "-------------------------------------------------------"
cdk -a "java -cp ${JAR_DIR}/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" deploy \
--require-approval never -c propertiesfile="${INSTANCE_PROPERTIES_PATH}" -c newinstance=true "*"
