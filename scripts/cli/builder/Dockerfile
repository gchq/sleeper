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

ARG DEPENDENCIES_IMAGE="sleeper-dependencies:current"
FROM ${DEPENDENCIES_IMAGE}

RUN apt-get update && apt-get install -y \
    maven \
    openjdk-17-jdk \
    openjdk-17-source \
    git \
    python3 \
    pip \
    python3.10-venv \
    zip \
    jq \
    sudo \
    gcc g++ cmake make \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
# Change install location because the default is in the home directory, which is overwritten by Dev Containers
# We also need to make it accessible to the non-root user
RUN adduser --disabled-password --gecos "" rust \
    && passwd -d rust
RUN mkdir /rust && chown rust:rust /rust
USER rust
ENV RUSTUP_HOME=/rust/.rustup
ENV CARGO_HOME=/rust/.cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y -c clippy,rustfmt
RUN $CARGO_HOME/bin/cargo install cross

USER root
COPY entrypoint.sh /entrypoint.sh

# Create non-root user
RUN adduser --disabled-password --gecos "" sleeper \
    && usermod -aG sudo,docker,rust sleeper \
    && passwd -d sleeper
USER sleeper

# Add Rust toolchain to path for non-interactive shells, as by default it is only added to .bashrc
ENV PATH=$CARGO_HOME/bin:$PATH
# Reset CARGO_HOME to use user registry
ENV CARGO_HOME=/home/sleeper/.cargo

# Set variable telling cross-rs Rust cross compiler that it is already inside a container
ENV CROSS_CONTAINER_IN_CONTAINER=true

# Set command line prompt to show you're in the builder Docker container
RUN echo 'export PS1="\[🐳\] \[\e[0;36m\]\u@sleeper-builder\[\e[0m\]: \w # "' >> ~/.bashrc

WORKDIR /sleeper-builder

ENTRYPOINT [ "/entrypoint.sh" ]
