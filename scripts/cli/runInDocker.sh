#!/usr/bin/env bash
# Copyright 2022-2025 Crown Copyright
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

HOME_IN_IMAGE=/home/sleeper
ALL_IMAGES=(sleeper-local sleeper-builder)

# Allow use of runner Dockerfile in source directory or in home install
THIS_DIR=$(cd "$(dirname "$0")" && pwd)
RUNNER_PATH="$THIS_DIR/runner"
HOME_RUNNER_PATH="$HOME/.sleeper/runner"
if [ ! -f "$RUNNER_PATH/Dockerfile" ]; then
    RUNNER_PATH="$HOME_RUNNER_PATH"
fi

run_in_docker() {
  local RUN_PARAMS
  RUN_PARAMS=()
  if [ -t 1 ]; then # Only pass TTY to Docker if connected to terminal
    RUN_PARAMS+=(-it)
  fi
  local TEMP_DIR=$(mktemp -d)
  mkdir -p "$HOME/.aws"
  RUN_PARAMS+=(
    --rm
    --add-host "host.docker.internal=host-gateway"
    -v /var/run/docker.sock:/var/run/docker.sock
    -v "$HOME/.aws:$HOME_IN_IMAGE/.aws"
    -v "$HOME/.ssh:$HOME_IN_IMAGE/.ssh"
    -v "$HOME/.cache:$HOME_IN_IMAGE/.cache"
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
  rmdir "$TEMP_DIR"
}

build_temp_runner_image() {
  local RUN_IMAGE=$1
  local TEMP_TAG=$(date +%Y-%m-%d"_"%H_%M_%S)_$RANDOM
  # Propagate current user IDs to image, to avoid mixed file ownership
  local SET_UID=$(id -u)
  local SET_GID=$(id -g)
  local SET_DOCKER_GID=$(getent group docker | cut -d: -f3)
  TEMP_RUNNER_IMAGE="sleeper-runner:$TEMP_TAG"
  echo "Propagating current user to Docker image"
  set +e
  docker build "$RUNNER_PATH" --quiet -t "$TEMP_RUNNER_IMAGE" \
    --build-arg RUN_IMAGE="$RUN_IMAGE" \
    --build-arg SET_UID=$SET_UID \
    --build-arg SET_GID=$SET_GID \
    --build-arg SET_DOCKER_GID=$SET_DOCKER_GID
  if [ $? -ne 0 ]; then
    echo "Failed docker build. Please run 'sleeper cli upgrade'."
    exit
  fi
  set -e
}

run_in_environment_docker() {
  build_temp_runner_image sleeper-local:current
  mkdir -p "$HOME/.sleeper/environments"
  run_in_docker \
    -v "$HOME/.sleeper/environments:$HOME_IN_IMAGE/.sleeper/environments" \
    "$TEMP_RUNNER_IMAGE" "$@"
  docker image remove "$TEMP_RUNNER_IMAGE" &> /dev/null
}

run_in_builder_docker() {
  build_temp_runner_image sleeper-builder:current
  mkdir -p "$HOME/.sleeper/builder"
  mkdir -p "$HOME/.m2"
  run_in_docker \
    -v "$HOME/.m2:$HOME_IN_IMAGE/.m2" \
    -v "$HOME/.sleeper/builder:/sleeper-builder" \
    -e HOST_MOUNT_PATH="$HOME/.sleeper/builder" \
    -e CONTAINER_MOUNT_PATH=/sleeper-builder \
    "$TEMP_RUNNER_IMAGE" "$@"
  docker image remove "$TEMP_RUNNER_IMAGE" &> /dev/null
}

get_version() {
  run_in_docker sleeper-local:current cat /sleeper/version.txt
}

pull_docker_images() {
  echo "Downloading CLI runner Dockerfile"
  mkdir -p "$HOME_RUNNER_PATH"
  curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/runner/Dockerfile" --output "$HOME_RUNNER_PATH/Dockerfile"

  echo "Pulling CLI Docker images"
  for IMAGE_NAME in "${ALL_IMAGES[@]}"; do
    echo "Pulling image: $IMAGE_NAME"
    REMOTE_IMAGE="ghcr.io/gchq/$IMAGE_NAME:latest"
    LOCAL_IMAGE="$IMAGE_NAME:current"

    docker pull "$REMOTE_IMAGE"
    docker tag "$REMOTE_IMAGE" "$LOCAL_IMAGE"
  done
}

find_docker_image_digests() {
  echo "Checking CLI image digests"
  declare -ga IMAGE_DIGESTS
  for IMAGE_NAME in "${ALL_IMAGES[@]}"; do
    IMAGE="$IMAGE_NAME:current"
    DIGEST="$(docker images -q "$IMAGE" 2> /dev/null)"
    if [ -n "$DIGEST" ]; then
      echo "Found digest for $IMAGE_NAME: $DIGEST"
      IMAGE_DIGESTS+=("$DIGEST")
    fi
  done
}

find_runner_image_digests() {
  echo "Checking runner image digests"
  declare -ga RUNNER_DIGESTS
  local LINES=$(docker images -q sleeper-runner 2> /dev/null)
  while read -r LINE
  do
    DUPLICATE=false
    for DIGEST in "${RUNNER_DIGESTS[@]}"; do
      if [[ "$DIGEST" == "$LINE" ]]; then
        DUPLICATE=true
        break
      fi
    done
    if [[ "$DUPLICATE" == "false" ]]; then
      echo "Found runner digest for cleanup: $LINE"
      RUNNER_DIGESTS+=("$LINE")
    fi
  done <<< "$LINES"
}

remove_old_images() {
  OLD_DIGESTS=("${IMAGE_DIGESTS[@]}")
  find_docker_image_digests
  echo "Cleaning up old CLI images"
  for OLD_DIGEST in "${OLD_DIGESTS[@]}"; do
    UPDATED=true
    for NEW_DIGEST in "${IMAGE_DIGESTS[@]}"; do
      if [[ "$OLD_DIGEST" == "$NEW_DIGEST" ]]; then
        UPDATED=false
        break
      fi
    done
    if [[ "$UPDATED" == "true" ]]; then
      docker image rm "$OLD_DIGEST"
    fi
  done
  find_runner_image_digests
  echo "Cleaning up old runner images"
  for DIGEST in "${RUNNER_DIGESTS[@]}"; do
    docker image rm -f "$DIGEST"
  done
}

upgrade_cli() {
  find_docker_image_digests
  echo "Updating CLI command"
  EXECUTABLE_PATH="${BASH_SOURCE[0]}"
  local TEMP_DIR=$(mktemp -d)
  TEMP_PATH="$TEMP_DIR/sleeper"
  curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/runInDocker.sh" --output "$TEMP_PATH"
  chmod a+x "$TEMP_PATH"
  "$TEMP_PATH" cli pull-images
  remove_old_images
  mv "$TEMP_PATH" "$EXECUTABLE_PATH"
  rmdir "$TEMP_DIR"
  echo "Updated"

  # If we didn't exit here, bash would carry on where it left off before the function call, but in the new version.
  # We want to avoid that because we can't predict what may have changed.
  # Since we're in a function, Bash will have read all of the function code from the old version, although the new
  # version may have changed.
  exit
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
