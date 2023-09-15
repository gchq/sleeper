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

FROM ubuntu:22.04

RUN apt-get update
RUN apt-get install -y curl

# Install NodeJS (for AWS CDK)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash
RUN apt-get install -y nodejs

# Install AWS CDK CLI
RUN npm install -g aws-cdk

# Install AWS CLI
WORKDIR /aws
RUN apt-get install -y unzip less
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "$dpkgArch" in \
        arm64) awsArch='aarch64' ;; \
        *) awsArch='x86_64' ;; \
    esac; \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-$awsArch.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN rm -rf /aws
WORKDIR /

# Install Java
RUN apt-get install -y openjdk-17-jre

# Install Docker
RUN apt-get -y install ca-certificates curl gnupg lsb-release
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
RUN echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get -y install docker-ce docker-ce-cli containerd.io

# Install Rust and C++
RUN apt-get install -y gcc g++ cmake make
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y -c clippy,rustfmt
RUN $HOME/.cargo/bin/cargo install cross

# Install text editors
RUN apt-get -y install nano vim emacs

# Enable autocompletion
RUN apt-get -y install bash-completion
