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

THIS_DIR=$(dirname "${BASH_SOURCE[0]}");
TEMP_DIR="${THIS_DIR}/temp"
GENERATED_DIR="${THIS_DIR}/temp/generated"
TEMPLATE_DIR="${THIS_DIR}/../../templates"
source "${THIS_DIR}/../propertiesUtils.sh"
source "${THIS_DIR}/runTestUtils.sh"

mkdir -p "${GENERATED_DIR}"
create_properties_file() {
  echo "test" > "${TEMP_DIR}/$1";
}
clear_temp_dirs(){
  rm -f "${TEMP_DIR}"/*.properties
  rm -f "${TEMP_DIR}/schema.json"
  rm "${GENERATED_DIR}/"*.properties
  rm "${GENERATED_DIR}/schema.json"
}
tearDown() {
  clear_temp_dirs;
  rmdir "$GENERATED_DIR"
  rmdir "$TEMP_DIR"
}
test_custom_properties_file(){
  create_properties_file "$1"
  copy_all_properties "$TEMP_DIR" "$GENERATED_DIR" "$TEMPLATE_DIR";
  grep -xq "test" "${GENERATED_DIR}/$1" || fail_test "custom $1 file was not copied" 
  clear_temp_dirs;
}

test_custom_properties_file "instance.properties"
test_custom_properties_file "tags.properties"
test_custom_properties_file "table.properties"
test_custom_properties_file "schema.json"

copy_all_properties "$TEMP_DIR" "$GENERATED_DIR" "$TEMPLATE_DIR";
grep -xq "sleeper.id=changeme" "${GENERATED_DIR}/instance.properties" || fail_test "template instance properties file was not copied" 

tearDown;