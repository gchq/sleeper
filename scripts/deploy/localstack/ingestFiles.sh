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
unset CDPATH

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <instance-id> <files>"
  exit 1
fi
INSTANCE_ID=$1;

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
DOCKER_DIR="$SCRIPTS_DIR/docker"
VERSION=$(cat "${SCRIPTS_DIR}/templates/version.txt")

INGEST_TASK_IMAGE="sleeper-ingest-runner"
echo "Building ingest-runner docker image"
docker build -t "$INGEST_TASK_IMAGE" "$DOCKER_DIR/ingest"

echo "Uploading files to source bucket and sending ingest job to queue"
java -cp "${SCRIPTS_DIR}/jars/clients-${VERSION}-utility.jar" sleeper.clients.docker.SendFilesToIngest "$@"

CONTAINER_NAME="sleeper-$INSTANCE_ID-ingest"
echo "Running ingest task in docker."
docker run --rm \
  -e AWS_ENDPOINT_URL=http://host.docker.internal:4566 \
  -e AWS_ACCESS_KEY_ID=test-access-key \
  -e AWS_SECRET_ACCESS_KEY=test-secret-key \
  --name="$CONTAINER_NAME" $INGEST_TASK_IMAGE "sleeper-$INSTANCE_ID-config"
echo "Ingest task complete"