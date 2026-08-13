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

REGISTRY=""
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --registry) REGISTRY="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done


TEMP_DIR=$(mktemp -d)
TEMP_PATH="$TEMP_DIR/sleeper"

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
LOCAL_SCRIPT="$THIS_DIR/runInDocker.sh"
if [ -f "$LOCAL_SCRIPT" ]; then
  echo "Local Sleeper CLI found, using that"
  SCRIPT_PATH="$LOCAL_SCRIPT"
else
  echo "Downloading Sleeper CLI"
  curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/runInDocker.sh" --output "$TEMP_PATH"
  SCRIPT_PATH="$TEMP_PATH"
  echo "Downloaded command"
fi
chmod a+x "$SCRIPT_PATH"

# Set registry if provided, overriding the default
if [ -n "$REGISTRY" ]; then
  "$SCRIPT_PATH" cli set-registry "$REGISTRY"
fi
"$SCRIPT_PATH" cli pull-images
echo "Downloaded Docker images"

EXECUTABLE_DIR="$HOME/.local/bin"
mkdir -p "$EXECUTABLE_DIR"
EXECUTABLE_PATH="$EXECUTABLE_DIR/sleeper"
cp "$SCRIPT_PATH" "$EXECUTABLE_PATH"
rmdir "$TEMP_DIR"
echo "Installed"

# Ensure executable directory is on path
case "$PATH" in
  *"$EXECUTABLE_DIR"*)
    echo "Executable directory already on path: $EXECUTABLE_DIR"
    ;;
  *)
    echo "Adding executable directory to path: $EXECUTABLE_DIR"
    if ! grep -q "$EXECUTABLE_DIR" "$HOME/.bashrc" 2> /dev/null; then
      echo "export PATH=\"\$PATH:$EXECUTABLE_DIR\"" >> "$HOME/.bashrc"
      echo "Added to ~/.bashrc"
    else
      echo "Already in ~/.bashrc"
    fi
    if ! grep -q "$EXECUTABLE_DIR" "$HOME/.zshrc" 2> /dev/null; then
      echo "export PATH=\"\$PATH:$EXECUTABLE_DIR\"" >> "$HOME/.zshrc"
      echo "Added to ~/.zshrc"
    else
      echo "Already in ~/.zshrc"
    fi
    echo "Please relaunch a terminal to be able to use Sleeper commands"
    ;;
esac
