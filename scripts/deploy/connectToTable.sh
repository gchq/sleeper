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

if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <instance-id> <table-name>"
	exit 1
fi

INSTANCE_ID=$1
TABLE_NAME=$2

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
GENERATED_DIR=${SCRIPTS_DIR}/generated
TEMPLATE_DIR=${SCRIPTS_DIR}/templates

echo "-------------------------------------------------------------------------------"
echo "Connecting to table"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "TABLE_NAME: ${TABLE_NAME}"
echo "GENERATED_DIR:${GENERATED_DIR}"
echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"

rm -rf ${GENERATED_DIR}
mkdir -p ${GENERATED_DIR}

#######################
# Output Bucket Names #
#######################
CONFIG_BUCKET=sleeper-${INSTANCE_ID}-config
echo "${CONFIG_BUCKET}" > ${GENERATED_DIR}/configBucket.txt
TABLE_BUCKET=sleeper-${INSTANCE_ID}-table-${TABLE_NAME}
echo "${TABLE_BUCKET}" > ${GENERATED_DIR}/tableBucket.txt
QUERY_BUCKET=sleeper-${INSTANCE_ID}-query-results
echo "${QUERY_BUCKET}" > ${GENERATED_DIR}/queryResultsBucket.txt

###################
# Read properties #
###################

echo "Reading properties from S3"
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
TABLE_PROPERTIES=${GENERATED_DIR}/table.properties
TAGS=${GENERATED_DIR}/tags.properties
SCHEMA=${GENERATED_DIR}/schema.json
aws s3api get-object --bucket ${CONFIG_BUCKET} --key config ${INSTANCE_PROPERTIES}
aws s3api get-object --bucket ${CONFIG_BUCKET} --key tables/${TABLE_NAME} ${TABLE_PROPERTIES}

# Tags
grep "^sleeper.tags=" ${INSTANCE_PROPERTIES} | cut -d'=' -f2 | sed 's/\([^,]\{1,\}\),\([^,]\{1,\}\),\{0,1\}/\1=\2\n/g' \
  > ${TAGS}

# Schema
grep "^sleeper.table.schema=" ${TABLE_PROPERTIES} | cut -d'=' -f2 | sed 's/\\:/:/g' \
  > ${SCHEMA}

# Overwrite references to local files
source "${SCRIPTS_DIR}/functions/sedInPlace.sh"
sed_in_place \
	-e "s|^sleeper.tags.file=.*|sleeper.tags.file=${TAGS}|" \
	-e "s|^sleeper.table.properties=.*|sleeper.table.properties=${TABLE_PROPERTIES}|" \
	${INSTANCE_PROPERTIES}
sed_in_place \
	-e "s|^sleeper.table.schema.file=.*|sleeper.table.schema.file=${SCHEMA}|" \
	${TABLE_PROPERTIES}
