#!/usr/bin/env bash
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

THIS_DIR=$(cd $(dirname $0) && pwd)
source "${THIS_DIR}/isInArray.sh"

A_B_C=("A" "B" "C")
is_in_array "A" A_B_C || echo "A should be in A_B_C"
is_in_array "B" A_B_C || echo "B should be in A_B_C"
is_in_array "C" A_B_C || echo "C should be in A_B_C"
is_in_array "Z" A_B_C && echo "Z should not be in A_B_C"

AB_C=("A B" "C")
is_in_array "A" AB_C && echo "A should not be in AB_C"
is_in_array "B" AB_C && echo "B should not be in AB_C"
is_in_array "A B" AB_C || echo "A B should be in AB_C"
is_in_array "C" AB_C || echo "C should be in AB_C"

if is_in_array "A" A_B_C; then
  echo "If construction can return true"
fi

if ! is_in_array "Z" A_B_C; then
  echo "If construction can return false"
fi

echo "Tests completed"
