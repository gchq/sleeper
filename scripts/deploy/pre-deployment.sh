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

#####################
# Initial variables #
#####################

if [ "$#" -ne 6 ]; then
	echo "Usage: $0 <instance-id> <vpc> <subnet> <table-name> <template-dir> <generated-dir>"
	exit 1
fi

INSTANCE_ID=$1
VPC=$2
SUBNET=$3
TABLE_NAME=$4
TEMPLATE_DIR=$5
GENERATED_DIR=$6

echo "-------------------------------------------------------------------------------"
echo "Running Pre-Deployment"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "VPC: ${VPC}"
echo "SUBNET:${SUBNET}"
echo "TABLE_NAME: ${TABLE_NAME}"
echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"
echo "GENERATED_DIR:${GENERATED_DIR}"

SLEEPER_JARS=${SLEEPER_JARS:-sleeper-${INSTANCE_ID}-jars}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
set +e
[ "${REGION}" = "" ] && REGION=$(aws configure get region)
[ "${REGION}" = "" ] && REGION=$(TOKEN=$(curl --silent -m 5 -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600") \
&& curl --silent -m 5 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/dynamic/instance-identity/document | grep region | cut -d'"' -f 4)
[ "${REGION}" = "" ] && REGION=$(curl --silent -m 5 http://169.254.169.254/latest/dynamic/instance-identity/document | grep region | cut -d'"' -f 4)
[ "${REGION}" = "" ] && echo "Unable to locate Region" && exit 1
set -e

DOCKER_REGISTRY=${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com
SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
JAR_DIR=${SCRIPTS_DIR}/jars
BASE_DOCKERFILE_DIR=${SCRIPTS_DIR}/docker
VERSION=$(cat "${TEMPLATE_DIR}/version.txt")

echo "Using Account: ${ACCOUNT}"
echo "Using Region: ${REGION}"
echo "SLEEPER_JARS: ${SLEEPER_JARS}"
echo "DOCKER_REGISTRY: ${DOCKER_REGISTRY}"
echo "SCRIPTS_DIR: ${SCRIPTS_DIR}"
echo "JAR_DIR: ${JAR_DIR}"
echo "VERSION: ${VERSION}"

#############################
# Generate Property files #
#############################

echo "Generating properties"
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
TABLE_PROPERTIES=${GENERATED_DIR}/table.properties
TAGS=${GENERATED_DIR}/tags.properties
SCHEMA=${GENERATED_DIR}/schema.json

# Tags
sed -e "s/Name=.*/Name=${INSTANCE_ID}/" \
 "${TEMPLATE_DIR}/tags.template" \
 > "${TAGS}"

# Schema
cp "${TEMPLATE_DIR}/schema.template" "${SCHEMA}"

# Table Properties
sed \
  -e "s|^sleeper.table.name=.*|sleeper.table.name=${TABLE_NAME}|" \
	"${TEMPLATE_DIR}/tableproperties.template" \
	> "${TABLE_PROPERTIES}"

# Instance Properties
# Note this sed command uses the file in generated dir not the template dir
# as this was required to include some pre-generated system test specific properties
source "${SCRIPTS_DIR}/functions/sedInPlace.sh"
sed_in_place \
	-e "s|^sleeper.account=.*|sleeper.account=${ACCOUNT}|" \
	-e "s|^sleeper.region=.*|sleeper.region=${REGION}|" \
	-e "s|^sleeper.id=.*|sleeper.id=${INSTANCE_ID}|" \
	-e "s|^sleeper.version=.*|sleeper.version=${VERSION}|" \
	-e "s|^sleeper.jars.bucket=.*|sleeper.jars.bucket=${SLEEPER_JARS}|" \
	-e "s|^sleeper.compaction.repo=.*|sleeper.compaction.repo=${INSTANCE_ID}/compaction-job-execution|" \
	-e "s|^sleeper.ingest.repo=.*|sleeper.ingest.repo=${INSTANCE_ID}/ingest|" \
	-e "s|^sleeper.bulk.import.eks.repo=.*|sleeper.bulk.import.eks.repo=${INSTANCE_ID}/bulk-import-runner|" \
	-e "s|^sleeper.vpc=.*|sleeper.vpc=${VPC}|" \
	-e "s|^sleeper.subnet=.*|sleeper.subnet=${SUBNET}|" \
	-e "s|^sleeper.tags.file=.*|sleeper.tags.file=${TAGS}|" \
	-e "s|^sleeper.table.properties=.*|sleeper.table.properties=${TABLE_PROPERTIES}|" \
	"${INSTANCE_PROPERTIES}"

###################################
# Build and publish Docker images #
###################################
STACKS=$(grep sleeper.optional.stacks "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)
"${SCRIPTS_DIR}/deploy/uploadDockerImages.sh" "${INSTANCE_ID}" "${DOCKER_REGISTRY}" "${VERSION}" "${STACKS}" "${BASE_DOCKERFILE_DIR}"

#####################
# Upload JARS to S3 #
#####################
"${SCRIPTS_DIR}/deploy/uploadJars.sh" "${SLEEPER_JARS}" "${REGION}" "${VERSION}" "${JAR_DIR}"

#######################
# Output Bucket Names #
#######################
CONFIG_BUCKET=sleeper-${INSTANCE_ID}-config
echo "${CONFIG_BUCKET}" > "${GENERATED_DIR}/configBucket.txt"
TABLE_BUCKET=sleeper-${INSTANCE_ID}-table-${TABLE_NAME}
echo "${TABLE_BUCKET}" > "${GENERATED_DIR}/tableBucket.txt"
QUERY_BUCKET=sleeper-${INSTANCE_ID}-query-results
echo "${QUERY_BUCKET}" > "${GENERATED_DIR}/queryResultsBucket.txt"
