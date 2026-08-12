#!/usr/bin/env bash
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

set -e
unset CDPATH

if [ "$#" -lt 1 ]; then
  echo "Usage: environment deploy <unique-id> <optional-cdk-parameters>"
  exit 1
fi

ENVIRONMENT_ID=$1
shift

if [ "$#" -lt 1 ]; then
  CDK_PARAMS=()
else
  CDK_PARAMS=("$@")
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
CDK_ROOT_DIR=$(cd "$THIS_DIR" && cd ../../app && pwd)
ENVIRONMENTS_DIR=$(cd "$HOME/.sleeper/environments" && pwd)
ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$ENVIRONMENT_ID"
OUTPUTS_FILE="$ENVIRONMENT_DIR/outputs.json"

# Find a tags parameter, if one was passed (e.g. -c tags=key,value,key,value)
TAGS_VALUE=""
TAGS_PRESENT=false
for param in "${CDK_PARAMS[@]}"; do
  case "$param" in
    tags=*)
      TAGS_PRESENT=true
      TAGS_VALUE="${param#tags=}"
      ;;
  esac
done

# When run interactively with tags, show them for confirmation. Tags are key,value pairs, so an
# odd number of entries or an empty key/value means a tag is missing its value; block and ask the
# user to run the command again. Non-interactive runs (e.g. automation) skip this; the CDK app
# validates the tags either way.
if [ "$TAGS_PRESENT" = true ] && [ -t 0 ]; then
  IFS=',' read -r -a TAG_ITEMS <<< "$TAGS_VALUE"
  TAGS_VALID=true
  echo ""
  echo "Tags to apply to all environment resources:"
  i=0
  while [ "$i" -lt "${#TAG_ITEMS[@]}" ]; do
    TAG_KEY="${TAG_ITEMS[$i]}"
    if [ $((i + 1)) -lt "${#TAG_ITEMS[@]}" ]; then
      TAG_VALUE="${TAG_ITEMS[$((i + 1))]}"
    else
      TAG_VALUE=""
    fi
    if [ -z "$TAG_KEY" ]; then
      TAG_KEY="(no key)"
      TAGS_VALID=false
    fi
    if [ -z "$TAG_VALUE" ]; then
      TAG_VALUE="(no value)"
      TAGS_VALID=false
    fi
    printf '  %s = %s\n' "$TAG_KEY" "$TAG_VALUE"
    i=$((i + 2))
  done
  echo ""
  if [ "$TAGS_VALID" != true ]; then
    echo "Each tag needs both a key and a value. Please run the command again with a value for every tag."
    exit 1
  fi
  read -r -p "Continue? [y/n] " TAGS_CONFIRM
  case "$TAGS_CONFIRM" in
    y | Y) ;;
    *)
      echo "Aborted."
      exit 1
      ;;
  esac
fi

pushd "$CDK_ROOT_DIR" > /dev/null
cdk deploy -c instanceId="$ENVIRONMENT_ID" --outputs-file "$OUTPUTS_FILE" --all "${CDK_PARAMS[@]}"
popd > /dev/null

USERNAME=$(jq ".[\"$ENVIRONMENT_ID-SleeperEnvironment\"].BuildEC2LoginUser" "$OUTPUTS_FILE" --raw-output)

echo "$ENVIRONMENT_ID" > "$ENVIRONMENTS_DIR/current.txt"
echo "$USERNAME" > "$ENVIRONMENTS_DIR/currentUser.txt"

# If an EC2 was created, wait for deployment, make a test connection to remember SSH certificate
INSTANCE_ID=$(jq ".[\"$ENVIRONMENT_ID-SleeperEnvironment\"].BuildEC2Id" "$OUTPUTS_FILE" --raw-output)
if [ "$INSTANCE_ID" != "null" ]; then
  "$THIS_DIR/test-connection.sh"
fi
