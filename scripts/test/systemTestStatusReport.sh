#!/usr/bin/env bash
# Copyright 2022-2024 Crown Copyright
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

SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
TEMPLATE_DIR=${SCRIPTS_DIR}/templates
GENERATED_DIR=${SCRIPTS_DIR}/generated
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)
VERSION=$(cat "${TEMPLATE_DIR}/version.txt")
TABLE_NAME="system-test"

java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" sleeper.clients.status.report.StatusReport "${INSTANCE_ID}" "${TABLE_NAME}"
