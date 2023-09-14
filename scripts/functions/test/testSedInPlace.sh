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

source "$(dirname "${BASH_SOURCE[0]}")/../sedInPlace.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runTestUtils.sh"

echo "Hello name!" > testSedInPlace.txt
sed_in_place -e "s|name|dude|" testSedInPlace.txt
expect_string_for_actual "Hello dude!" "$(cat testSedInPlace.txt)"
rm testSedInPlace.txt

end_tests
