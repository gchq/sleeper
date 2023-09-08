
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
source "$(dirname "${BASH_SOURCE[0]}")/../tableUtils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runTestUtils.sh"

PROPERTIES_DIR="$(dirname "${BASH_SOURCE[0]}")/tmp-properties"

# Test with one table
# Given
TABLE_DIR="$PROPERTIES_DIR/tables/only"
mkdir -p "$TABLE_DIR"
echo "sleeper.table.name=test-table" > "$TABLE_DIR/table.properties"

# When
IFS=" " read -r -a TABLE_NAMES <<< "$(list_table_names "$PROPERTIES_DIR")"

# Then
EXPECTED_NAMES=("test-table")
array_equals TABLE_NAMES EXPECTED_NAMES || fail_test "TABLE_NAMES should be (${EXPECTED_NAMES[*]}). Found (${TABLE_NAMES[*]})"

rm -rf "$PROPERTIES_DIR"

# Test with multiple tables
# Given
TABLE_1_DIR="$PROPERTIES_DIR/tables/first"
TABLE_2_DIR="$PROPERTIES_DIR/tables/second"
mkdir -p "$TABLE_1_DIR"
mkdir -p "$TABLE_2_DIR"
echo "sleeper.table.name=table-1" > "$TABLE_1_DIR/table.properties"
echo "sleeper.table.name=table-2" > "$TABLE_2_DIR/table.properties"

# When
IFS=" " read -r -a TABLE_NAMES <<< "$(list_table_names "$PROPERTIES_DIR")"

# Then
EXPECTED_NAMES=("table-1" "table-2")
array_equals TABLE_NAMES EXPECTED_NAMES || fail_test "TABLE_NAMES should be (${EXPECTED_NAMES[*]}). Found (${TABLE_NAMES[*]})"

rm -rf "$PROPERTIES_DIR"

end_tests
