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

SCRIPTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)
GENERATED_DIR="$SCRIPTS_DIR/generated"

read_instance_property() {
  java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar sleeper.status.config.ReadInstanceProperty "$GENERATED_DIR" "$@"
}

read_table_property() {
  java -cp "${SCRIPTS_DIR}"/jars/clients-*-utility.jar sleeper.status.config.ReadTableProperty "$GENERATED_DIR" "$@"
}

read_system_test_property() {
  java -cp "${SCRIPTS_DIR}"/jars/system-test-*-utility.jar sleeper.systemtest.util.ReadSystemTestProperty "$GENERATED_DIR" "$@"
}
