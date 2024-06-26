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

set -e
unset CDPATH

if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <shortId> <vpc> <csv-list-of-subnets> <test> <optional-maven-params>"
  exit 1
fi

SHORT_ID=$1
VPC=$2
SUBNETS=$3
TEST=$4
shift 4

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

"$THIS_DIR/deployTest.sh" "$SHORT_ID" "$VPC" "$SUBNETS" -Dsleeper.system.test.cluster.enabled=true "-DrunIT=$TEST" "$@"
