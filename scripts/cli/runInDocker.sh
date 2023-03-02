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

HOME_IN_IMAGE=/root

run_in_docker() {
  local RUN_PARAMS
  RUN_PARAMS=()
  if [ -t 1 ]; then # Only pass TTY to Docker if connected to terminal
    RUN_PARAMS+=(-it)
  fi
  RUN_PARAMS+=(
    --rm
    -v /var/run/docker.sock:/var/run/docker.sock
    -v "$HOME/.aws:$HOME_IN_IMAGE/.aws"
    -e AWS_ACCESS_KEY_ID
    -e AWS_SECRET_ACCESS_KEY
    -e AWS_SESSION_TOKEN
    -e AWS_PROFILE
    -e AWS_REGION
    -e AWS_DEFAULT_REGION
    -e ID
    -e VPC
    -e SUBNET
    "$@"
  )
  docker run "${RUN_PARAMS[@]}"
}

run_in_environment_docker() {
  run_in_docker \
    -v "$HOME/.sleeper/environments:$HOME_IN_IMAGE/.sleeper/environments" \
    sleeper-local:current "$@"
}

run_in_deployment_docker() {
  run_in_docker \
    -v "$HOME/.sleeper/generated:/sleeper/generated" \
    sleeper-deployment:current "$@"
}

run_in_builder_docker() {
  run_in_docker \
    -v "$HOME/.sleeper/builder:/sleeper-builder" \
    -v "$HOME/.m2:$HOME_IN_IMAGE/.m2" \
    sleeper-builder:current "$@"
}

get_version() {
  run_in_environment_docker cat /sleeper/version.txt
}

upgrade_cli() {
  if [ "$#" -lt 1 ]; then
    CURRENT_VERSION=$(get_version | tr -d '\r\n')
    case $CURRENT_VERSION in
    *-SNAPSHOT)
      VERSION="latest"
      ;;
    *)
      # We could get the latest version from GitHub by querying this URL:
      # https://github.com/gchq/sleeper/releases/latest
      # At time of writing, this shows no releases. Once we have full releases on GitHub, we could use that.
      echo "Please specify version to upgrade to"
      return 1
      ;;
    esac
  else
    VERSION=$1
  fi

  GIT_REF="$VERSION"
  REMOTE_TAG="$VERSION"
  if [ "$VERSION" == "main" ]; then
    REMOTE_TAG="latest"
  elif [ "$VERSION" == "latest" ]; then
    GIT_REF="main"
  elif [[ "$VERSION" == "v"* ]]; then # Strip v from start of version number for Docker
    REMOTE_TAG=${VERSION:1}
  fi

  pull_and_tag sleeper-local
  pull_and_tag sleeper-builder
  pull_and_tag sleeper-deployment

  echo "Updating CLI command"
  EXECUTABLE_PATH="${BASH_SOURCE[0]}"
  curl "https://raw.githubusercontent.com/gchq/sleeper/$GIT_REF/scripts/cli/runInDocker.sh" --output "$EXECUTABLE_PATH"
  chmod a+x "$EXECUTABLE_PATH"
  echo "Updated"
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
elif [ "$COMMAND" == "builder" ]; then
  run_in_builder_docker "$@"
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
    upgrade_cli "$@"
  else
    echo "Command not found: cli $SUBCOMMAND"
    exit 1
  fi
else
  echo "Command not found: $COMMAND"
  exit 1
fi
