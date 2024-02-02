#!/usr/bin/env bash
#
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
#

set -e
unset CDPATH

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <instance-id>"
  exit 1
fi
INSTANCE_ID=$1;

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
DOCKER_DIR="$SCRIPTS_DIR/docker"
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

COMPACTION_JOB_EXECUTION_IMAGE="sleeper-compaction-job-execution"
echo "Building compaction-job-runner docker image"
docker build -t "$COMPACTION_JOB_EXECUTION_IMAGE" "$DOCKER_DIR/compaction-job-execution"

echo "Running compaction job creation"
java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" sleeper.clients.status.update.CompactFiles "$@"

CONTAINER_NAME="sleeper-$INSTANCE_ID-compaction-job-execution"
echo "Running compaction task in docker."
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -e AWS_ENDPOINT_URL \
  -e AWS_ACCESS_KEY_ID=test-access-key \
  -e AWS_SECRET_ACCESS_KEY=test-secret-key \
  --name="$CONTAINER_NAME" $COMPACTION_JOB_EXECUTION_IMAGE "sleeper-$INSTANCE_ID-config"
echo "Compaction task complete"