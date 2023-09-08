
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

list_table_names() {
  local NAMES=()
  local PROPERTIES_DIR=$1
  for dir in "$PROPERTIES_DIR"/tables/*
  do
    local TABLE_PROPERTIES="$dir/table.properties"
    NAMES+=("$(get_table_name_from_file "$TABLE_PROPERTIES")")
  done
  echo "${NAMES[*]}"
}

get_table_name_from_file(){
  grep "sleeper.table.name" "$1" | cut -d'=' -f2
}
