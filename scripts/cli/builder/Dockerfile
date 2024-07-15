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
    gcc g++ cmake make \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
# Change install location because the default is in the home directory, which is overwritten by Dev Containers
ENV RUSTUP_HOME=/rust/.rustup
ENV CARGO_HOME=/rust/.cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y -c clippy,rustfmt
RUN $CARGO_HOME/bin/cargo install cross

# Set command line prompt to show you're in the builder Docker container
RUN echo 'export PS1="\[🐳\] \[\e[0;36m\]\u@sleeper-builder\[\e[0m\]: \w # "' >> ~/.bashrc

# Set variable telling cross-rs Rust cross compiler that it is already inside a container
RUN echo 'export CROSS_CONTAINER_IN_CONTAINER=true' >> ~/.bashrc

# Tell the builder docker container image to take it's host name from the actual docker container ID, not the assigned host name
# This is a workaround for how cross-rs works when it's running in "docker-in-docker" mode. In DinD mode, this line:
# https://github.com/cross-rs/cross/blob/3bfc6d54c817a2991f610d258f3290906c97474f/src/docker/shared.rs#L1354 causes it to read
# the HOSTNAME environment variable so it can call "docker inspect" to read some mount paths. Docker normally sets a container's
# HOSTNAME to be the ID of the running container, but as this builder image usually runs with --network=host then the container
# receives the host's hostname causing cross-rs to call docker inspect and get an error back. To workaround, we configure this
# builder image to save it's container ID in a specific file which we can now read back in here. See scripts/cli/runInDocker.sh for
# the other part of this workaround.
RUN echo 'export HOSTNAME=$(cat /tmp/container.id)' >> ~/.bashrc

WORKDIR /sleeper-builder

CMD [ "bash" ]
SHELL [ "bash", "-i", "-l", "-c" ]
