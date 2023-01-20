#!/bin/bash
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

set -e # Exit on error
set -u # Treat unset variable as error
set -x # Trace commands
set -o pipefail

LOGIN_USER=${loginUser}
LOGIN_HOME=/home/$LOGIN_USER

# Get Updates
export DEBIAN_FRONTEND=noninteractive
sudo apt update && sudo apt -y dist-upgrade

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
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin binfmt-support qemu-user-static

# Install AWS tools
sudo apt install -y unzip htop
curl -sS "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
unzip -qq "/tmp/awscliv2.zip" -d "/tmp" && sh /tmp/aws/install --update && rm -rf "/tmp/awscliv2.zip" "/tmp/aws"

# Allow user to access docker socket
usermod -aG docker $LOGIN_USER

# Install JDK & Maven
sudo apt install -y maven

# Install CDK
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm install -g npm
sudo npm install -g aws-cdk

# Check out code
if [ ! -d $LOGIN_HOME/${repository} ]; then
  runuser -u $LOGIN_USER -- git clone -b ${branch} https://github.com/${fork}/${repository}.git $LOGIN_HOME/${repository}
fi

if [ -f /var/run/reboot-required ]; then
  /sbin/shutdown -r now && exit
fi
