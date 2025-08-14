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

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
PROJECT_ROOT=$(cd "$SCRIPTS_DIR" && cd .. && pwd)

COMPACTION_DOCKER_DIR="$SCRIPTS_DIR/docker/compaction-job-execution"
cp "$PROJECT_ROOT/java/compaction/compaction-job-execution/docker/Dockerfile" "$COMPACTION_DOCKER_DIR"
docker build -t "compaction-job-execution:test" "$COMPACTION_DOCKER_DIR"

pushd "$PROJECT_ROOT/java"
echo "Running..."
mvn verify -PsystemTest -DskipRust=true -pl clients "-DrunIT=DockerImageTest*"
popd
