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

if [ "$#" -lt 4 ] || [ "$#" -gt 6 ]; then
  echo "Usage: $0 <instance-id> <vpc> <subnet> <table-name> <optional-deploy-paused-flag> <optional-split-points-file>"
  exit 1
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
GENERATED_DIR="${SCRIPTS_DIR}/generated"
TEMPLATES_DIR="${SCRIPTS_DIR}/templates"
VERSION=$(cat "${TEMPLATES_DIR}/version.txt")

source "${SCRIPTS_DIR}/functions/propertiesUtils.sh"
copy_all_properties "${THIS_DIR}" "${GENERATED_DIR}" "${TEMPLATES_DIR}"

java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" sleeper.clients.deploy.DeployNewInstance "${SCRIPTS_DIR}" "$@"
EXIT_CODE=$?

if [ "${EXIT_CODE}" -eq 0 ]; then
  rm "${THIS_DIR}/instance.properties"
  rm "${THIS_DIR}/tags.properties"
  rm "${THIS_DIR}/table.properties"
  rm "${THIS_DIR}/schema.properties"
fi;
