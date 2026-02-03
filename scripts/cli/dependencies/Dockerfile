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

FROM ubuntu:24.04

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    unzip \
    less \
    nano \
    vim \
    emacs \
    bash-completion \
    man-db \
    unminimize \
    && yes | unminimize \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /etc/ssl/private/ssl-cert-snakeoil.key \
    && userdel -r ubuntu \
    && groupdel systemd-journal
# The deleted user and group are added either by one of the above packages, or by unminimize. They are deleted to avoid
# conflicts when propagating the host user and group IDs.
# The ubuntu user is deleted because it conflicts with the sleeper user.
# The systemd-journal group is deleted because it conflicts with the docker group.

RUN mkdir -p /etc/apt/keyrings

# Install Amazon Corretto JDK
RUN curl -fsSL https://apt.corretto.aws/corretto.key | gpg --dearmor --yes -o /etc/apt/keyrings/corretto-keyring.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main"  \
    | tee /etc/apt/sources.list.d/corretto.list > /dev/null
RUN apt-get update && apt-get install -y --no-install-recommends \
    java-17-amazon-corretto-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install NodeJS (for AWS CDK)
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor --yes -o /etc/apt/keyrings/nodesource.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main"  \
    | tee /etc/apt/sources.list.d/nodesource.list > /dev/null
RUN apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g npm@latest \
    && npm install -g tar@7.5.7

# Install AWS CDK CLI
RUN npm install -g aws-cdk

# Install AWS CLI
WORKDIR /aws
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "$dpkgArch" in \
    arm64) awsArch='aarch64' ;; \
    *) awsArch='x86_64' ;; \
    esac; \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-$awsArch.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip && rm awscliv2.zip && ./aws/install && rm -rf /aws
WORKDIR /

# Install Docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
RUN echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

RUN apt-get update && apt-get install -y --no-install-recommends \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    && rm -rf /var/lib/apt/lists/*
