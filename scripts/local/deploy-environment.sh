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
	echo "Usage: $0 <uniqueId> <optional_cdk_parameters>"
	exit 1
fi

INSTANCE_ID=$1

if [ "$#" -lt 2 ]; then
	CDK_PARAMS=("--all")
else
  CDK_PARAMS=("$@")
fi

ENVIRONMENTS_DIR="./environments"

cdk deploy -c instanceId="$INSTANCE_ID" --outputs-file "$ENVIRONMENTS_DIR/$INSTANCE_ID-outputs.json" "${CDK_PARAMS[@]}"
mv "$INSTANCE_ID-BuildEC2.pem" "$ENVIRONMENTS_DIR"
echo "$INSTANCE_ID" > "$ENVIRONMENTS_DIR/current.txt"
