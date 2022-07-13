#!/usr/bin/env bash
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

BASE_DIR=$(cd $(dirname $0) && cd "../../" && pwd)

TEMPLATE_DIR=${BASE_DIR}/scripts/templates
GENERATED_DIR=${BASE_DIR}/scripts/generated
INSTANCE_PROPERTIES_PATH=${GENERATED_DIR}/instance.properties
JAR_DIR=${BASE_DIR}/scripts/jars


echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"
echo "GENERATED_DIR:${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES: ${INSTANCE_PROPERTIES_PATH}"
echo "BASE_DIR: ${BASE_DIR}"
echo "JAR_DIR: ${JAR_DIR}"

VERSION=$(cat "${TEMPLATE_DIR}/version.txt")
echo "VERSION: ${VERSION}"

mkdir -p ${GENERATED_DIR}

echo "Copying instance properties template into generated files for use in pre-deployment steps"
cp ${TEMPLATE_DIR}/instanceproperties.template ${INSTANCE_PROPERTIES_PATH}

echo "Running Pre-deployment steps"
${BASE_DIR}/scripts/deploy/pre-deployment.sh ${INSTANCE_ID} ${VPC} ${SUBNET} ${TABLE_NAME} ${TEMPLATE_DIR} ${GENERATED_DIR}

echo "-------------------------------------------------------"
echo "Deploying Stacks"
echo "-------------------------------------------------------"
cdk -a "java -cp ${JAR_DIR}/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" deploy \
--require-approval never -c propertiesfile=${GENERATED_DIR}/instance.properties -c validate=true "*"
