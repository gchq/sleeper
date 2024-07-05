#!/bin/bash
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
sudo apt remove -y docker.io containerd runc
sudo apt install -y ca-certificates curl gnupg lsb-release
sudo mkdir -p /etc/apt/keyrings
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor --batch -o /etc/apt/keyrings/docker.gpg
fi
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
export DEBIAN_FRONTEND=noninteractive
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io binfmt-support qemu-user-static

# Allow user to access docker socket
usermod -aG docker "$LOGIN_USER"

# Install Sleeper CLI
curl "https://raw.githubusercontent.com/$FORK/$REPOSITORY/$BRANCH/scripts/cli/install.sh" -o "$LOGIN_HOME/sleeper-install.sh"
chmod +x "$LOGIN_HOME/sleeper-install.sh"
runuser --login "$LOGIN_USER" -c "$LOGIN_HOME/sleeper-install.sh $BRANCH"

# Check out code
if [ ! -d "$LOGIN_HOME/$REPOSITORY" ]; then
  runuser --login "$LOGIN_USER" -c "curl -O https://raw.githubusercontent.com/$FORK/$REPOSITORY/$BRANCH/shell.nix"
  runuser --login "$LOGIN_USER" -c "nix-shell --run \"git clone -b $BRANCH https://github.com/$FORK/$REPOSITORY.git\""
fi

if [ -f /var/run/reboot-required ]; then
  /sbin/shutdown -r now && exit
fi
