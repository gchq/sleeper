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
  echo "$@"
  ((TEST_FAILURES++))
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
  echo "$((TEST_FAILURES)) failures"
  if [[ $TEST_FAILURES -gt 0 ]]; then
    exit 1
  fi
}

expect_string_for_actual() {
  if [[ "$1" != "$2" ]]; then
    fail_test "Expected '$1', got $2"
  fi
}

start_tests
