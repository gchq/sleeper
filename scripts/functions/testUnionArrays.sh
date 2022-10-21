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
source "${THIS_DIR}/arrayUtils.sh"

A_B_C=("A" "B" "C")
C_D=("C" "D")
C=("C")

union_arrays A_B_C C_D found_ABC_CD
array_equals C found_ABC_CD || echo "found_ABC_CD should be C, was ${found_A_B_C_D[@]}"

union_arrays A_B_C A_B_C found_ABC_ABC
array_equals A_B_C found_ABC_ABC || echo "found_ABC_ABC should be A_B_C, was ${found_ABC_ABC[@]}"

union_arrays C_D C found_CD_C
array_equals C found_CD_C || echo "found_CD_C should be C, was ${found_CD_C[@]}"
