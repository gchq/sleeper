#!/usr/bin/env bash
# Copyright 2022-2025 Crown Copyright
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
unset CDPATH

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <instance-id> <table-name>"
  exit 1
fi

INSTANCE_ID=$1
TABLE_NAME=$2

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)

TEMPLATE_DIR=${SCRIPTS_DIR}/templates
GENERATED_DIR=${SCRIPTS_DIR}/generated
JAR_DIR=${SCRIPTS_DIR}/jars
TABLE_DIR=${GENERATED_DIR}/tables/${TABLE_NAME}
TABLE_PROPERTIES=${TABLE_DIR}/table.properties
SCHEMA=${TABLE_DIR}/schema.json

VERSION=$(cat "${TEMPLATE_DIR}/version.txt")

echo "Generating properties"

mkdir -p "${TABLE_DIR}"

# Schema
cp "${TEMPLATE_DIR}/schema.template" "${SCHEMA}"

# Table Properties
sed \
  -e "s|^sleeper.table.name=.*|sleeper.table.name=${TABLE_NAME}|" \
  "${TEMPLATE_DIR}/tableproperties.template" \
  > "${TABLE_PROPERTIES}"

echo "-------------------------------------------------------"
echo "Adding table"
echo "-------------------------------------------------------"
java -cp "${JAR_DIR}/clients-${VERSION}-utility.jar" sleeper.clients.table.AddTable "${INSTANCE_ID}" "${TABLE_PROPERTIES}"
