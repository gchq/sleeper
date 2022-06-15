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

if [ "$#" -ne 4 ]; then
	echo "Usage: $0 <uniqueId> <vpc> <subnet> <table-name>"
	exit 1
fi

INSTANCE_ID=$1
VPC=$2
SUBNET=$3
TABLE_NAME=$4

echo "-------------------------------------------------------------------------------"
echo "Running Build & Deploy"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "VPC: ${VPC}"
echo "SUBNET:${SUBNET}"
echo "TABLE_NAME: ${TABLE_NAME}"

SCRIPTS_DIR=$(cd $(dirname $0) && pwd)
BASE_DIR=$(cd $(dirname $0) && cd "../../" && pwd)

echo "-------------------------------------------------------------------------------"
echo "Building Project"
echo "-------------------------------------------------------------------------------"
pushd ${BASE_DIR}/java
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
mvn -q clean install -Pquick

mkdir -p ${BASE_DIR}/scripts/jars
mkdir -p ${BASE_DIR}/scripts/docker
cp  ${BASE_DIR}/java/distribution/target/distribution-${VERSION}-bin/scripts/jars/* ${BASE_DIR}/scripts/jars/
cp -r ${BASE_DIR}/java/distribution/target/distribution-${VERSION}-bin/scripts/docker/* ${BASE_DIR}/scripts/docker/
cp  ${BASE_DIR}/java/distribution/target/distribution-${VERSION}-bin/scripts/templates/version.txt ${BASE_DIR}/scripts/templates/version.txt

${SCRIPTS_DIR}/deploy.sh ${INSTANCE_ID} ${VPC} ${SUBNET} ${TABLE_NAME}
