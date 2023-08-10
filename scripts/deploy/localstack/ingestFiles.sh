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

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <instance-id> <files>"
  exit 1
fi
INSTANCE_ID=$1;

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")
DOCKER_DIR="$SCRIPTS_DIR/docker"

echo "Uploading files to source bucket and sending ingest job to queue"
java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" sleeper.clients.docker.SendFilesToIngest "$@"

INGEST_TASK_IMAGE="sleeper-ingest-runner"
CONTAINER_NAME="sleeper-$INSTANCE_ID-ingest"
echo "Running ingest task in docker. You can follow the logs by viewing the docker container \"$CONTAINER_NAME\""
docker run --rm \
  -e AWS_ENDPOINT_URL=http://host.docker.internal:4566 \
  -e AWS_ACCESS_KEY_ID=test-access-key \
  -e AWS_SECRET_ACCESS_KEY=test-secret-key \
  -d --name="$CONTAINER_NAME" $INGEST_TASK_IMAGE "sleeper-$INSTANCE_ID-config"