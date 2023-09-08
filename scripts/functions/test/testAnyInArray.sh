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

A=("A")
Z_A=("Z" "A")
A_B_C=("A" "B" "C")
D_E_F=("D" "E" "F")
any_in_array A A_B_C || fail_test "A should be in A_B_C"
any_in_array Z_A A_B_C || fail_test "Z_A should be in A_B_C"
any_in_array A D_E_F && fail_test "A should not be in D_E_F"
any_in_array A_B_C D_E_F && fail_test "A_B_C should not be in D_E_F"

end_tests
