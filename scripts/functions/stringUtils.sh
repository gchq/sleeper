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

seconds_to_str() {
  local TOTAL_SECONDS=$1
  local SECONDS=$((TOTAL_SECONDS%60))
  local MINUTES=$((TOTAL_SECONDS/60%60))
  local HOURS=$((TOTAL_SECONDS/60/60))
  local MESSAGES=()
  [[ $HOURS -gt 0 ]] && MESSAGES+=("$(pluralise $HOURS "hour")")
  [[ $MINUTES -gt 0 ]] && MESSAGES+=("$(pluralise $MINUTES "minute")")
  [[ ${#MESSAGES} -eq 0 || $SECONDS -gt 0 ]] && MESSAGES+=("$(pluralise $SECONDS "second")")
  echo "${MESSAGES[@]}"
}

pluralise() {
  local NUMBER=$1
  local DESCRIPTOR=$2
  if [[ $NUMBER -eq 1 ]]; then
    echo "1 $DESCRIPTOR"
  else
    echo "$NUMBER ${DESCRIPTOR}s"
  fi
}
