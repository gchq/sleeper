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

if [ "$#" -lt 1 ]; then
	echo "Usage: environment destroy <uniqueId> <optional_cdk_parameters>"
	exit 1
fi

INSTANCE_ID=$1

if [ "$#" -lt 2 ]; then
	CDK_PARAMS=("--all")
  DELETE_ENVIRONMENT_DIR=true
else
  CDK_PARAMS=("$@")
  DELETE_ENVIRONMENT_DIR=false
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd .. && pwd)
CDK_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
ENVIRONMENTS_DIR=$(cd "$THIS_DIR" && cd ../../environments && pwd)
ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$INSTANCE_ID"

"$SCRIPTS_DIR/util/configure-aws.sh"

pushd "$CDK_DIR" > /dev/null
cdk destroy -c instanceId="$INSTANCE_ID" "${CDK_PARAMS[@]}"
popd > /dev/null

if [ "$DELETE_ENVIRONMENT_DIR" = true ]; then
  rm -rf "$ENVIRONMENT_DIR"
fi
