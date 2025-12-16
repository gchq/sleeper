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

set -e # Exit on error
set -u # Treat unset variable as error
set -x # Trace commands
set -o pipefail

# These variables are set from Java, treating this script as a template
LOGIN_USER=${loginUser}
REPOSITORY=${repository}
FORK=${fork}
BRANCH=${branch}

LOGIN_HOME=/home/$LOGIN_USER

# Get Updates
export DEBIAN_FRONTEND=noninteractive
sudo apt update && sudo apt -y dist-upgrade

# Install Nix
if [ ! -d /nix ]; then
  runuser --login "$LOGIN_USER" -c "curl -L https://nixos.org/nix/install | sh -s -- --daemon --yes"
fi

# Install latest Docker
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
  sudo apt remove -y docker.io containerd runc
  sudo apt install -y ca-certificates curl gnupg lsb-release
  sudo mkdir -p /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor --batch -o /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  export DEBIAN_FRONTEND=noninteractive
  sudo apt update
  sudo apt install -y docker-ce docker-ce-cli containerd.io binfmt-support qemu-user-static

  # Allow user to access docker socket
  usermod -aG docker "$LOGIN_USER"
fi

# Install Sleeper CLI
if [ ! -f "$LOGIN_HOME/.local/bin/sleeper" ]; then
  curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/install.sh" -o "$LOGIN_HOME/sleeper-install.sh"
  chmod +x "$LOGIN_HOME/sleeper-install.sh"
  runuser --login "$LOGIN_USER" -c "$LOGIN_HOME/sleeper-install.sh"
fi

# Check out code
REPOSITORY_DIR="$LOGIN_HOME/.sleeper/builder/$REPOSITORY"
if [ ! -d "$REPOSITORY_DIR" ]; then
  runuser --login "$LOGIN_USER" -c "sleeper builder git clone -b $BRANCH https://github.com/$FORK/$REPOSITORY.git"
fi

CRONTAB_FILE="/sleeper-init/crontab"
NIGHTLY_TEST_SETTINGS_FILE="$LOGIN_HOME/.sleeper/builder/nightlyTestSettings.json"
if [ -f "$CRONTAB_FILE" ] && [ ! -f "$NIGHTLY_TEST_SETTINGS_FILE" ]; then
  runuser --login "$LOGIN_USER" -c "cp /sleeper-init/nightlyTestSettings.json $NIGHTLY_TEST_SETTINGS_FILE"
  chown "$LOGIN_USER:$LOGIN_USER" "$NIGHTLY_TEST_SETTINGS_FILE"
  runuser --login "$LOGIN_USER" -c "crontab $CRONTAB_FILE"
fi

if [ -f /var/run/reboot-required ]; then
  /sbin/shutdown -r now && exit
fi
