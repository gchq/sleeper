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
  local ARRAY_NAME=$2[@]
  local ARRAY=("${!ARRAY_NAME}")
  for item in "${ARRAY[@]}"; do
    [[ "$FIND_VALUE" == "$item" ]] && return 0
  done
  return 1
}

any_in_array() {
  local FIND_NAME=$1[@]
  local ARRAY_NAME=$2[@]
  local FIND=("${!FIND_NAME}")
  local ARRAY=("${!ARRAY_NAME}")
  for find_item in "${FIND[@]}"; do
    for array_item in "${ARRAY[@]}"; do
      [[ "$find_item" == "$array_item" ]] && return 0
    done
  done
  return 1
}

union_arrays() {
  local NAME_1=$1[@]
  local NAME_2=$2[@]
  local NAME_OUT=$3
  local ARRAY_1=("${!NAME_1}")
  local ARRAY_2=("${!NAME_2}")
  declare -ga ${NAME_OUT}
  add_to_array ${NAME_OUT} $(echo ${ARRAY_1[@]} ${ARRAY_2[@]} | tr ' ' '\n' | sort | uniq -d | tr '\n' ' ')
}

add_to_array() {
  local -n ARRAY=$1
  for item in ${@:2}; do
    ARRAY[${#ARRAY[@]}]=$item
  done
}

array_equals() {
  local NAME_1=$1[@]
  local NAME_2=$2[@]
  local ARRAY_1=("${!NAME_1}")
  local ARRAY_2=("${!NAME_2}")
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
