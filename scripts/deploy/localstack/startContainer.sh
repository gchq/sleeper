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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
docker compose -f "$THIS_DIR/docker-compose.yml" up -d 
echo "Running localstack container on port 4566"
echo "To use sleeper with this container, set the AWS_ENDPOINT_URL environment variable:"
if [ "$IN_CLI_CONTAINER" = "true" ]; then
  echo "export AWS_ENDPOINT_URL=http://host.docker.internal:4566"
else
  echo "export AWS_ENDPOINT_URL=http://localhost:4566"
fi
echo ""
echo "To revert to using the default AWS endpoint, you can unset this environment variable:"
echo "unset AWS_ENDPOINT_URL"