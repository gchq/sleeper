
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
BASE_DIR=$(cd "$(dirname "$0")" && cd "../../" && pwd)
MAVEN_DIR="$BASE_DIR/java"
SCRIPTS_DIR="$BASE_DIR/scripts"
JARS_DIR="$SCRIPTS_DIR/jars"
DOCKER_DIR="$SCRIPTS_DIR/docker"
VERSION_FILE="$SCRIPTS_DIR/templates/version.txt"

"$SCRIPTS_DIR/build/build.sh" "$@"

VERSION=$(cat "$VERSION_FILE")

cp -r "$MAVEN_DIR/system-test/system-test-data-generation/docker" "$DOCKER_DIR/system-test"
cp -r "$MAVEN_DIR/system-test/system-test-data-generation/target/system-test-data-generation-${VERSION}-utility.jar" "$DOCKER_DIR/system-test/system-test.jar"
cp -r "$MAVEN_DIR/system-test/system-test-drivers/target/system-test-drivers-${VERSION}-utility.jar" "$JARS_DIR/system-test-${VERSION}-utility.jar"
