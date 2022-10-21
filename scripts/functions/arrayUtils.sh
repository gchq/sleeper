# Copyright 2022 Crown Copyright
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

is_in_array() {
  TEST_VALUE=$1
  TEST_ARRAY_NAME=$2[@]
  TEST_ARRAY=("${!TEST_ARRAY_NAME}")
  for item in "${TEST_ARRAY[@]}"; do
    [[ "$TEST_VALUE" == "$item" ]] && return 0
  done
  return 1
}

any_in_array() {
  TEST_FIND_NAME=$1[@]
  TEST_ARRAY_NAME=$2[@]
  TEST_FIND=("${!TEST_FIND_NAME}")
  TEST_ARRAY=("${!TEST_ARRAY_NAME}")
  for find_item in "${TEST_FIND[@]}"; do
    for array_item in "${TEST_ARRAY[@]}"; do
      [[ "$find_item" == "$array_item" ]] && return 0
    done
  done
  return 1
}
