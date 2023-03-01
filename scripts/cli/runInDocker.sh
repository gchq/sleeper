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
  echo "Usage: sleeper <command>"
  exit 1
fi

run_in_environment_docker() {
  HOME_IN_IMAGE=/root

  docker run -it --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$HOME/.sleeper/environments:$HOME_IN_IMAGE/.sleeper/environments" \
    -v "$HOME/.aws:$HOME_IN_IMAGE/.aws" \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e AWS_SESSION_TOKEN \
    -e AWS_PROFILE \
    -e AWS_REGION \
    -e AWS_DEFAULT_REGION \
    sleeper-local:current "$@"
}

run_in_deployment_docker() {
  HOME_IN_IMAGE=/root

  docker run -it --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$HOME/.sleeper/generated:/sleeper/generated" \
    -v "$HOME/.aws:$HOME_IN_IMAGE/.aws" \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e AWS_SESSION_TOKEN \
    -e AWS_PROFILE \
    -e AWS_REGION \
    -e AWS_DEFAULT_REGION \
    sleeper-deployment:current "$@"
}

get_version() {
  run_in_environment_docker cat /sleeper/version.txt
}

upgrade_cli() {
  VERSION=$(get_version | tr -d '\r\n')
  case $VERSION in
  *-SNAPSHOT) # Handle main branch
    REMOTE_TAG=latest
    GIT_REF=main
    ;;
  *) # Handle release version
    REMOTE_TAG=$VERSION
    GIT_REF="v$VERSION"
    ;;
  esac
  pull_and_tag sleeper-local
  pull_and_tag sleeper-deployment

  echo "Updating Sleeper CLI"
  EXECUTABLE_PATH="${BASH_SOURCE[0]}"
  curl "https://raw.githubusercontent.com/gchq/sleeper/$GIT_REF/scripts/cli/runInDocker.sh" --output "$EXECUTABLE_PATH"
  chmod a+x "$EXECUTABLE_PATH"
  echo "Installed"
}

pull_and_tag() {
  IMAGE_NAME=$1
  REMOTE_IMAGE="ghcr.io/gchq/$IMAGE_NAME:$REMOTE_TAG"
  LOCAL_IMAGE="$IMAGE_NAME:current"

  docker pull "$REMOTE_IMAGE"
  docker tag "$REMOTE_IMAGE" "$LOCAL_IMAGE"
}

COMMAND=$1
shift

if [ "$COMMAND" == "aws" ]; then
  run_in_environment_docker aws "$@"
elif [ "$COMMAND" == "cdk" ]; then
  run_in_environment_docker cdk "$@"
elif [ "$COMMAND" == "version" ] || [ "$COMMAND" == "--version" ] || [ "$COMMAND" == "-v" ]; then
  get_version
elif [ "$COMMAND" == "deployment" ]; then
  run_in_deployment_docker "$@"
elif [ "$COMMAND" == "environment" ]; then
  if [ "$#" -eq 0 ]; then
    run_in_environment_docker
  else
    run_in_environment_docker environment "$@"
  fi
elif [ "$COMMAND" == "cli" ]; then
  SUBCOMMAND=$1
  shift
  if [ "$SUBCOMMAND" == "upgrade" ]; then
    upgrade_cli
  else
    echo "Command not found: cli $SUBCOMMAND"
    exit 1
  fi
else
  echo "Command not found: $COMMAND"
  exit 1
fi
