# Copyright 2022-2026 Crown Copyright
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
source "$(dirname "${BASH_SOURCE[0]}")/../jvmUtils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runTestUtils.sh"

EMPTY_ARRAY=()

# No JVM options set
read_jvm_options instance table --all
EXPECTED_JVM=("-XX:+ExitOnOutOfMemoryError")
EXPECTED_APP=("instance" "table" "--all")
array_equals EXPECTED_JVM JVM_OPTIONS || fail_test "Expected only exit on out of memory, got ${JVM_OPTIONS[*]}"
array_equals EXPECTED_APP APP_ARGS || fail_test "Expected all arguments for app, got ${APP_ARGS[*]}"

# JVM memory set between app arguments
read_jvm_options instance --jvm-memory 512m table --detailed "job 1" --force
EXPECTED_JVM=("-XX:+ExitOnOutOfMemoryError" "-Xmx512m")
EXPECTED_APP=("instance" "table" "--detailed" "job 1")
array_equals EXPECTED_JVM JVM_OPTIONS || fail_test "Expected 512m JVM memory, got ${JVM_OPTIONS[*]}"
array_equals EXPECTED_APP APP_ARGS || fail_test "Expected JVM options removed from app arguments, got ${APP_ARGS[*]}"

# Memory in gigabytes, skipping the available memory check
read_jvm_options --jvm-memory 9999G --force
EXPECTED_JVM=("-XX:+ExitOnOutOfMemoryError" "-Xmx9999G")
array_equals EXPECTED_JVM JVM_OPTIONS || fail_test "Expected 9999G JVM memory, got ${JVM_OPTIONS[*]}"
array_equals EMPTY_ARRAY APP_ARGS || fail_test "Expected no app arguments, got ${APP_ARGS[*]}"

# Invalid JVM memory values exit with an error. These run in a subshell as the function exits the shell.
(read_jvm_options --jvm-memory 8x --force) > /dev/null && fail_test "Expected unit 'x' to be rejected"
(read_jvm_options --jvm-memory 8 --force) > /dev/null && fail_test "Expected missing unit to be rejected"
(read_jvm_options --jvm-memory 100k --force) > /dev/null && fail_test "Expected unit 'k' to be rejected"
(read_jvm_options --jvm-memory 1.5g --force) > /dev/null && fail_test "Expected decimal to be rejected"
(read_jvm_options --jvm-memory) > /dev/null && fail_test "Expected missing value to be rejected"

# Run Java with a stand-in for the java command, which records its arguments and exits with JAVA_EXIT_CODE
java() {
  JAVA_ARGS=("$@")
  return "$JAVA_EXIT_CODE"
}
read_jvm_options --jvm-memory 512m --force instance table

# Passes JVM options before the given arguments
JAVA_EXIT_CODE=0
OUTPUT=$(run_java -cp app.jar MainClass "${APP_ARGS[@]}"; echo "exit=$?")
run_java -cp app.jar MainClass "${APP_ARGS[@]}"
EXPECTED_JAVA=("-XX:+ExitOnOutOfMemoryError" "-Xmx512m" "-cp" "app.jar" "MainClass" "instance" "table")
array_equals EXPECTED_JAVA JAVA_ARGS || fail_test "Expected JVM options then arguments, got ${JAVA_ARGS[*]}"
expect_string_for_actual "exit=0" "$OUTPUT"

# Explains how to set the memory when the JVM runs out of memory
JAVA_EXIT_CODE=3
OUTPUT=$(run_java -cp app.jar MainClass; echo "exit=$?")
expect_string_for_actual "Ran out of memory. You can give it more memory with --jvm-memory, e.g. --jvm-memory 8g
exit=3" "$OUTPUT"

# Other failures are returned without the memory explanation
JAVA_EXIT_CODE=1
OUTPUT=$(run_java -cp app.jar MainClass; echo "exit=$?")
expect_string_for_actual "exit=1" "$OUTPUT"

unset -f java

end_tests
