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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
JAVA_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)
pushd "$JAVA_DIR"
VERSION="$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)"
popd

java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" sleeper.clients.docker.TearDownDockerInstance "$@"
EXIT_CODE=$?
if [[ $EXIT_CODE -eq 0 ]]; then
  echo "Successfully tore down instance"
else
  echo "Failed to tear down instance"
fi