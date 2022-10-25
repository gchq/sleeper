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
AB_C=("A B" "C")
C_D=("C" "D")
C=("C")
ONE_TWO=(1 2)
TWO_THREE=(2 3)
TWO=(2)

union_arrays_to_variable A_B_C C_D found_ABC_CD
array_equals C found_ABC_CD || echo "found_ABC_CD should be C, was ${found_ABC_CD[@]}"

union_arrays_to_variable A_B_C A_B_C found_ABC_ABC
array_equals A_B_C found_ABC_ABC || echo "found_ABC_ABC should be A_B_C, was ${found_ABC_ABC[@]}"

union_arrays_to_variable C_D C found_CD_C
array_equals C found_CD_C || echo "found_CD_C should be C, was ${found_CD_C[@]}"

union_arrays_to_variable AB_C A_B_C found_AB_C_ABC
array_equals C found_AB_C_ABC || echo "found_AB_C_ABC should be C, was ${found_AB_C_ABC[@]}"

found_predeclared=(1 2 3)
union_arrays_to_variable A_B_C C_D found_predeclared
array_equals C found_predeclared || echo "found_predeclared should be C, was ${found_predeclared[@]}"

union_arrays_to_variable ONE_TWO TWO_THREE found_12_23
array_equals TWO found_12_23 || echo "found_12_23 should be 2, was ${found_12_23[@]}"
