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

VERSION="develop"

echo "Downloading Sleeper CLI"
TEMP_DIR=$(mktemp -d)
TEMP_PATH="$TEMP_DIR/sleeper"
curl "https://raw.githubusercontent.com/gchq/sleeper/$VERSION/scripts/cli/runInDocker.sh" --output "$TEMP_PATH"
chmod a+x "$TEMP_PATH"
echo "Downloaded command"

"$TEMP_PATH" cli pull-images "$VERSION"
echo "Downloaded Docker images"

EXECUTABLE_DIR="$HOME/.local/bin"
mkdir -p "$EXECUTABLE_DIR"
EXECUTABLE_PATH="$EXECUTABLE_DIR/sleeper"
mv "$TEMP_PATH" "$EXECUTABLE_PATH"
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
