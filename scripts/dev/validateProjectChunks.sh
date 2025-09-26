
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
PROJECT_ROOT=$(dirname "$(dirname "${THIS_DIR}")")

CHUNKS_YAML="$PROJECT_ROOT/.github/config/chunks.yaml"
MAVEN_PROJECT="$PROJECT_ROOT/java"
pushd "$MAVEN_PROJECT/build"
mvn compile exec:java -q -e -Dexec.mainClass=sleeper.build.chunks.ValidateProjectChunks \
    -Dexec.args="$CHUNKS_YAML $MAVEN_PROJECT"
popd
