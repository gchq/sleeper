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

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <instance-id>"
	exit 1
fi

INSTANCE_ID=$1

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
GENERATED_DIR=${SCRIPTS_DIR}/generated
TEMPLATE_DIR=${SCRIPTS_DIR}/templates

echo "-------------------------------------------------------------------------------"
echo "Connecting to table"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "GENERATED_DIR:${GENERATED_DIR}"
echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"

rm -rf "${GENERATED_DIR:?}"/*
mkdir -p "${GENERATED_DIR}"

#######################
# Output Bucket Names #
#######################
CONFIG_BUCKET=sleeper-${INSTANCE_ID}-config
echo "${CONFIG_BUCKET}" > "${GENERATED_DIR}"/configBucket.txt
QUERY_BUCKET=sleeper-${INSTANCE_ID}-query-results
echo "${QUERY_BUCKET}" > "${GENERATED_DIR}"/queryResultsBucket.txt

###################
# Read properties #
###################

echo "Reading properties from S3"
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
TAGS=${GENERATED_DIR}/tags.properties

aws s3api get-object --bucket "${CONFIG_BUCKET}" --key config "${INSTANCE_PROPERTIES}"

# Tags
grep "^sleeper.tags=" "${INSTANCE_PROPERTIES}" | cut -d'=' -f2 | sed 's/\([^,]\{1,\}\),\([^,]\{1,\}\),\{0,1\}/\1=\2\n/g' \
  > "${TAGS}"

# Tables
TABLE_OBJECTS="$(aws s3api list-objects-v2 --bucket "${CONFIG_BUCKET}" --prefix tables/ --output text | tail -n +2 | cut -d$'\t' -f3)"
echo "$TABLE_OBJECTS" | while IFS='' read -r table_object; do

  TABLE_DIR=${GENERATED_DIR}/${table_object}
  TABLE_PROPERTIES=${TABLE_DIR}/table.properties
  SCHEMA=${TABLE_DIR}/schema.json
  mkdir -p "$TABLE_DIR"

  aws s3api get-object --bucket "${CONFIG_BUCKET}" --key "${table_object}" "${TABLE_PROPERTIES}"

  # Data bucket name
  grep "^sleeper.table.data.bucket=" "${TABLE_PROPERTIES}" | cut -d'=' -f2 \
    > "${TABLE_DIR}/tableBucket.txt"

  # Schema
  grep "^sleeper.table.schema=" "${TABLE_PROPERTIES}" | cut -d'=' -f2 | sed 's/\\:/:/g' \
    > "${SCHEMA}"

done
