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
  local TEMP_DIR=$(mktemp -d)
  local CONTAINER_ID_PATH="$TEMP_DIR/container.id"
  # We ensure the container ID is available as a file inside the container
  # See scripts/cli/builder/Dockerfile for why
  RUN_PARAMS+=(
    --rm
    --cidfile "$CONTAINER_ID_PATH"
    -v "$CONTAINER_ID_PATH:/tmp/container.id"
    --add-host "host.docker.internal=host-gateway"
    -v /var/run/docker.sock:/var/run/docker.sock
    -v "$HOME/.aws:$HOME_IN_IMAGE/.aws"
    -e "IN_CLI_CONTAINER=true"
    -e AWS_ACCESS_KEY_ID
    -e AWS_SECRET_ACCESS_KEY
    -e AWS_SESSION_TOKEN
    -e AWS_PROFILE
    -e AWS_REGION
    -e AWS_DEFAULT_REGION
    -e ID
    -e INSTANCE_ID
    -e VPC
    -e SUBNET
    "$@"
  )
  docker run "${RUN_PARAMS[@]}"
  rm "$CONTAINER_ID_PATH"
  rmdir "$TEMP_DIR"
}

run_in_environment_docker() {
  run_in_docker \
    -v "$HOME/.sleeper/environments:$HOME_IN_IMAGE/.sleeper/environments" \
    sleeper-local:current "$@"
}

run_in_builder_docker() {
  # Builder directory is mounted twice to work around a problem with the Rust cross compiler in WSL, which causes it to
  # look for the source code at its path in the host: https://github.com/cross-rs/cross/issues/728
  run_in_docker \
    -v "$HOME/.sleeper/builder:/sleeper-builder" \
    -v "$HOME/.sleeper/builder:$HOME/.sleeper/builder" \
    -v "$HOME/.m2:$HOME_IN_IMAGE/.m2" \
    sleeper-builder:current "$@"
}

get_version() {
  run_in_environment_docker cat /sleeper/version.txt
}

pull_docker_images(){
  pull_and_tag sleeper-local
  pull_and_tag sleeper-builder
}

upgrade_cli() {
  echo "Updating CLI command"
  EXECUTABLE_PATH="${BASH_SOURCE[0]}"
  local TEMP_DIR=$(mktemp -d)
  TEMP_PATH="$TEMP_DIR/sleeper"
  curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/runInDocker.sh" --output "$TEMP_PATH"
  chmod a+x "$TEMP_PATH"
  "$TEMP_PATH" cli pull-images
  mv "$TEMP_PATH" "$EXECUTABLE_PATH"
  rmdir "$TEMP_DIR"
  echo "Updated"

  # If we didn't exit here, bash would carry on where it left off before the function call, but in the new version.
  # We want to avoid that because we can't predict what may have changed.
  # Since we're in a function, Bash will have read all of the function code from the old version, although the new
  # version may have changed.
  exit
}

pull_and_tag() {
  IMAGE_NAME=$1
  REMOTE_IMAGE="ghcr.io/gchq/$IMAGE_NAME:latest"
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
    upgrade_cli
  elif [ "$SUBCOMMAND" == "pull-images" ]; then
    pull_docker_images
  else
    echo "Command not found: cli $SUBCOMMAND"
    exit 1
  fi
else
  echo "Command not found: $COMMAND"
  exit 1
fi
