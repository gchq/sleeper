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
unset CDPATH

is_in_array() {
  local FIND_VALUE=$1
  local ARRAY_REF="$2[@]"
  for item in "${!ARRAY_REF}"; do
    [[ "$FIND_VALUE" == "$item" ]] && return 0
  done
  return 1
}

any_in_array() {
  local FIND_REF="$1[@]"
  for find_item in "${!FIND_REF}"; do
    is_in_array "$find_item" "$2" && return 0
  done
  return 1
}

union_arrays_to_variable() {
  local ARRAY_1_REF="$1[@]"
  local ARRAY_2_NAME=$2
  local ARRAY_OUT_NAME=$3
  eval "$ARRAY_OUT_NAME=()"
  for item in "${!ARRAY_1_REF}"; do
    if is_in_array "$item" "$ARRAY_2_NAME"; then
      eval "$ARRAY_OUT_NAME+=(\"$item\")"
    fi
  done
}

array_equals() {
  # Use eval to get lengths of named arrays
  local LENGTH_1 LENGTH_2
  LENGTH_1=$(eval "echo \${#$1[@]}")
  LENGTH_2=$(eval "echo \${#$2[@]}")
  if [[ $LENGTH_1 -ne $LENGTH_2 ]]; then
    return 1
  fi
  for ((i=0;i<=LENGTH_1;i++)); do
    local ARRAY_1_REF="$1[$i]"
    local ARRAY_2_REF="$2[$i]"
    [[ "${!ARRAY_1_REF}" != "${!ARRAY_2_REF}" ]] && return 1
  done
  return 0
}
