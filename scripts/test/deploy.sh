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
	echo "Usage: $0 <properties_template> <uniqueId> <vpc> <subnet>"
	exit 1
fi

PROPERTIES_TEMPLATE=$1
INSTANCE_ID=$2
VPC=$3
SUBNET=$4

TABLE_NAME="system-test"
THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(dirname "$THIS_DIR")
JARS_DIR="$SCRIPTS_DIR/jars"

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Configuring Deployment"
echo "-------------------------------------------------------------------------------"
TEMPLATE_DIR="$SCRIPTS_DIR/templates"
GENERATED_DIR="$SCRIPTS_DIR/generated"
INSTANCE_PROPERTIES="$GENERATED_DIR/instance.properties"
VERSION=$(cat "$TEMPLATE_DIR/version.txt")
SYSTEM_TEST_JAR="${JARS_DIR}/system-test-${VERSION}-utility.jar"

mkdir -p "$GENERATED_DIR"

echo "Creating System Test Specific Instance Properties Template"
sed \
  -e "s|^sleeper.systemtest.repo=.*|sleeper.systemtest.repo=${INSTANCE_ID}/system-test|" \
  -e "s|^sleeper.optional.stacks=.*|sleeper.optional.stacks=CompactionStack,GarbageCollectorStack,PartitionSplittingStack,QueryStack,SystemTestStack,IngestStack,EmrBulkImportStack|" \
  -e "s|^sleeper.retain.infra.after.destroy=.*|sleeper.retain.infra.after.destroy=false|" \
  "$PROPERTIES_TEMPLATE" > "${INSTANCE_PROPERTIES}"

echo "PROPERTIES_TEMPLATE: ${PROPERTIES_TEMPLATE}"
echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"
echo "VERSION: ${VERSION}"
echo "GENERATED_DIR: ${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES: ${INSTANCE_PROPERTIES}"

echo "Starting Pre-deployment steps"
"${SCRIPTS_DIR}/deploy/pre-deployment.sh" "${INSTANCE_ID}" "${VPC}" "${SUBNET}" "${TABLE_NAME}" "${TEMPLATE_DIR}" "${GENERATED_DIR}"

END_CONFIGURE_DEPLOYMENT_TIME=$(record_time)
echo "Configuring deployment finished at $(recorded_time_str "$END_CONFIGURE_DEPLOYMENT_TIME"), took $(elapsed_time_str "$START_TIME" "$END_CONFIGURE_DEPLOYMENT_TIME")"

echo "-------------------------------------------------------------------------------"
echo "Deploying Stack"
echo "-------------------------------------------------------------------------------"
cdk -a "java -cp ${SYSTEM_TEST_JAR} sleeper.systemtest.cdk.SystemTestApp" deploy \
--require-approval never -c testpropertiesfile="${INSTANCE_PROPERTIES}" -c validate=true "*"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished deploy"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Configuring deployment finished at $(recorded_time_str "$END_CONFIGURE_DEPLOYMENT_TIME"), took $(elapsed_time_str "$START_TIME" "$END_CONFIGURE_DEPLOYMENT_TIME")"
echo "Stack deployment finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_CONFIGURE_DEPLOYMENT_TIME" "$FINISH_TIME")"
echo "Overall, deploy took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
