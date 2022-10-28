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

fail_test() {
  echo "$(find_test_source):${LINENO}" "$@"
  ((TEST_FAILURES++))
}

find_test_source() {
  local NUM_SOURCES=${#BASH_SOURCE[@]}
  local LAST_SOURCE=${BASH_SOURCE[$((NUM_SOURCES-1))]}
  if [[ "${LAST_SOURCE}" == *runAllTests.sh ]]; then
    basename "${BASH_SOURCE[$((NUM_SOURCES-2))]}"
  else
    basename "${LAST_SOURCE}"
  fi
}

start_tests() {
  ((TESTS_NESTING++))
}

end_tests() {
  ((TESTS_NESTING--))
  if [[ $TESTS_NESTING -eq 0 ]]; then
    report_test_results
  fi
}

report_test_results() {
  echo "$((TEST_FAILURES)) failure(s)"
  if [[ $TEST_FAILURES -gt 0 ]]; then
    exit 1
  fi
}

expect_string_for_actual() {
  expect_arguments 2 "$@"
  if [[ "$1" != "$2" ]]; then
    fail_test "Expected '$1', got $2"
  fi
}

expect_non_empty_string() {
  expect_arguments 1 "$@"
  local STRING=$1
  if [[ ${#STRING} -lt 1 ]]; then
    fail_test "Expected non-empty string"
  fi
}

expect_arguments() {
  local EXPECTED=$1
  local FOUND=$(($#-1))
  if [[ ${FOUND} -ne ${EXPECTED} ]]; then
    fail_test "${FUNCNAME[1]} needs ${EXPECTED} argument(s), got ${FOUND}"
  fi
}

start_tests
