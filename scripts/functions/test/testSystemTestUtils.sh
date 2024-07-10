#!/usr/bin/env bash
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

source "$(dirname "${BASH_SOURCE[0]}")/../systemTestUtils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runTestUtils.sh"

echo "instance-1" > testInstanceIds.txt
echo "instance-2" >> testInstanceIds.txt
EXPECTED=("instance-1" "instance-2")
SHORT_ID="in"
EXPECTED_SHORT_INSTANCE_NAMES=("stance-1" "stance-2")

PREDEFINED_ARRAY=()
read_instance_ids_to_array testInstanceIds.txt PREDEFINED_ARRAY
array_equals PREDEFINED_ARRAY EXPECTED || fail_test "PREDEFINED_ARRAY should equal EXPECTED"

read_instance_ids_to_array testInstanceIds.txt NEW_ARRAY
array_equals NEW_ARRAY EXPECTED || fail_test "NEW_ARRAY should equal EXPECTED"

read_short_instance_names_from_instance_ids_to_array "$SHORT_ID" testInstanceIds.txt PREDEFINED_ARRAY
array_equals PREDEFINED_ARRAY EXPECTED_SHORT_INSTANCE_NAMES || fail_test "PREDEFINED_ARRAY should equal EXPECTED_SHORT_INSTANCE_NAMES"

read_short_instance_names_from_instance_ids_to_array "$SHORT_ID" testInstanceIds.txt NEW_ARRAY
array_equals NEW_ARRAY EXPECTED_SHORT_INSTANCE_NAMES || fail_test "NEW_ARRAY should equal EXPECTED_SHORT_INSTANCE_NAMES"

rm testInstanceIds.txt

end_tests
