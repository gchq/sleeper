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

source "$(dirname "${BASH_SOURCE[0]}")/../timeUtils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runTestUtils.sh"

expect_non_empty_string "$(time_str)"

START=$(record_time)
END=$(record_time)
expect_non_empty_string "$(elapsed_time_str "${START}" "${END}")"
expect_non_empty_string "$(recorded_time_str "${START}")"

expect_string_for_actual "1970-01-01 00:00:00 UTC" "$(recorded_time_str 0)"
expect_string_for_actual "2022-10-28 11:57:10 UTC" "$(recorded_time_str 1666958230)"
expect_string_for_actual "Fri Oct 28 11:57:10 2022" "$(recorded_time_str 1666958230 "%c")"

expect_string_for_actual "1970-01-01 00:00:10 UTC" "$(recorded_time_str 10)"
expect_string_for_actual "1970-01-01 00:00:30 UTC" "$(recorded_time_str 30)"
expect_string_for_actual "20 seconds" "$(elapsed_time_str 10 30)"

expect_string_for_actual "0 seconds" "$(seconds_to_str 0)"
expect_string_for_actual "42 seconds" "$(seconds_to_str 42)"
expect_string_for_actual "1 minute" "$(seconds_to_str 60)"
expect_string_for_actual "1 minute 1 second" "$(seconds_to_str 61)"
expect_string_for_actual "1 hour 1 minute 1 second" "$(seconds_to_str $((60*60+61)))"
expect_string_for_actual "100 hours 1 minute 1 second" "$(seconds_to_str $((100*60*60+61)))"
expect_string_for_actual "1 hour" "$(seconds_to_str $((60*60)))"

end_tests
