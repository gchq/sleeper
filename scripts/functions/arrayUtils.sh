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
  local FIND_VALUE=$1
  local -n ARRAY=$2
  for item in "${ARRAY[@]}"; do
    [[ "$FIND_VALUE" == "$item" ]] && return 0
  done
  return 1
}

any_in_array() {
  local -n FIND=$1
  local -n ARRAY=$2
  for find_item in "${FIND[@]}"; do
    for array_item in "${ARRAY[@]}"; do
      [[ "$find_item" == "$array_item" ]] && return 0
    done
  done
  return 1
}

union_arrays_to_variable() {
  local -n ARRAY_1=$1
  local -n ARRAY_2=$2
  local NAME_OUT=$3
  declare -ga ${NAME_OUT}
  local -n ARRAY_OUT=${NAME_OUT}
  for item_1 in "${ARRAY_1[@]}"; do
    for item_2 in "${ARRAY_2[@]}"; do
      if [[ "$item_1" == "$item_2" ]]; then
        ARRAY_OUT[${#ARRAY_OUT[@]}]=$item_1
      fi
    done
  done
}

array_equals() {
  local -n ARRAY_1=$1
  local -n ARRAY_2=$2
  local LENGTH_1=${#ARRAY_1[@]}
  local LENGTH_2=${#ARRAY_2[@]}
  if [ $LENGTH_1 != $LENGTH_2 ]; then
    return 1
  fi
  for ((i=0;i<=$LENGTH_1;i++)); do
    if [[ ${ARRAY_1[i]} != ${ARRAY_2[i]} ]]; then
      return 1
    fi
  done
  return 0
}
