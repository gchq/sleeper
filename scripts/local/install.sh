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
  VERSION="latest"
else
  VERSION="$1"
fi

GIT_REF="$VERSION"
REMOTE_TAG="$VERSION"
if [ "$VERSION" == "main" ]; then
  REMOTE_TAG="latest"
elif [ "$VERSION" == "latest" ]; then
  GIT_REF="main"
fi

REMOTE_IMAGE="ghcr.io/gchq/sleeper-local:$REMOTE_TAG"
LOCAL_IMAGE="sleeper-local:current"

docker pull "$REMOTE_IMAGE"
docker tag "$REMOTE_IMAGE" "$LOCAL_IMAGE"

# Ensure executable directory is on path
EXECUTABLE_DIR="$HOME/.local/bin"
mkdir -p "$EXECUTABLE_DIR"
case "$PATH" in
  *"$EXECUTABLE_DIR"*)
    ;;
  *)
    if ! grep -q "$EXECUTABLE_DIR" "$HOME/.bashrc"; then
      echo "export PATH=\"\$PATH:$EXECUTABLE_DIR\"" >> "$HOME/.bashrc"
    fi
    ;;
esac

echo "Installing Sleeper CLI to user path"
EXECUTABLE_PATH="$EXECUTABLE_DIR/sleeper"
curl "https://raw.githubusercontent.com/gchq/sleeper/$GIT_REF/scripts/local/runInDocker.sh" --output "$EXECUTABLE_PATH"
chmod a+x "$EXECUTABLE_PATH"
