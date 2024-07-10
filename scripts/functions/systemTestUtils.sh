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
unset CDPATH

source "$(dirname "${BASH_SOURCE[0]}")/arrayUtils.sh"

read_short_instance_names_from_instance_ids() {
    local SHORT_ID=$1
    local INSTANCE_IDS_FILE=$2
    local DELIM=""
    while read -r id; do
      local SHORT_INSTANCE_NAME=$(echo "$id" | cut -b$(("${#SHORT_ID}" + 1))-)
      printf "%s" "$DELIM$SHORT_INSTANCE_NAME"
      DELIM=","
    done <"$INSTANCE_IDS_FILE"
}
