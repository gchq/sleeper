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

set -e

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

# Wait for deployment, make a test connection to remember SSH certificate
RETRY_NUM=30
RETRY_EVERY=10
NUM=$RETRY_NUM
until "$THIS_DIR/connect.sh" echo '"Test connection successful"'; do
  echo 1>&2 "Failed test connection with status $?, retrying $NUM more times, next in $RETRY_EVERY seconds"
  sleep $RETRY_EVERY
  ((NUM--))

  if [ $NUM -eq 0 ]; then
    echo 1>&2 "Test connection unsuccessful after $RETRY_NUM tries"
    exit 1
  fi
done
