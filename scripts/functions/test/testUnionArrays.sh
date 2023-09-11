#!/usr/bin/env bash
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

source "$(dirname "${BASH_SOURCE[0]}")/../arrayUtils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runTestUtils.sh"

A_B_C=("A" "B" "C")
AB_C=("A B" "C")
C_D=("C" "D")
C=("C")
ONE_TWO=(1 2)
TWO_THREE=(2 3)
TWO=(2)

union_arrays_to_variable A_B_C C_D found_ABC_CD
array_equals C found_ABC_CD || fail_test "found_ABC_CD should be C, was ${found_ABC_CD[@]}"

union_arrays_to_variable A_B_C A_B_C found_ABC_ABC
array_equals A_B_C found_ABC_ABC || fail_test "found_ABC_ABC should be A_B_C, was ${found_ABC_ABC[@]}"

union_arrays_to_variable C_D C found_CD_C
array_equals C found_CD_C || fail_test "found_CD_C should be C, was ${found_CD_C[@]}"

union_arrays_to_variable AB_C A_B_C found_AB_C_ABC
array_equals C found_AB_C_ABC || fail_test "found_AB_C_ABC should be C, was ${found_AB_C_ABC[@]}"

found_predeclared=(1 2 3)
union_arrays_to_variable A_B_C C_D found_predeclared
array_equals C found_predeclared || fail_test "found_predeclared should be C, was ${found_predeclared[@]}"

union_arrays_to_variable ONE_TWO TWO_THREE found_12_23
array_equals TWO found_12_23 || fail_test "found_12_23 should be 2, was ${found_12_23[@]}"

end_tests
