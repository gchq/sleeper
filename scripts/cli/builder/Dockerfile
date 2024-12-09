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
    git \
    python3 \
    python3-pip \
    python3-venv \
    zip \
    jq \
    uuid-runtime \
    sudo \
    gcc g++ cmake make \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
# Change install location because the default is in the home directory, which is overwritten by Dev Containers.
# We need to make it accessible to the non-root user. If we installed Rust directly as that user, we would need to
# update the file ownership of the whole Rust installation when we change the user's UID/GID.
# We install Rust as a separate, real user to own the install directory. We don't use a system user for this because the
# Rust install script expects to run in a real user account.
RUN groupadd -g 2500 rust \
    && adduser --disabled-password --gecos "" --uid 2500 --gid 2500 rust \
    && passwd -d rust
RUN mkdir /rust && chown rust:rust /rust
USER rust
ENV RUSTUP_HOME=/rust/.rustup
ENV CARGO_HOME=/rust/.cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y -c clippy,rustfmt
RUN $CARGO_HOME/bin/rustup component add rust-std-x86_64-unknown-linux-gnu rust-std-aarch64-unknown-linux-gnu rust-src
RUN $CARGO_HOME/bin/cargo install cross
RUN $CARGO_HOME/bin/cargo install cargo-audit --features=fix
USER root
RUN chmod -R a+rw $RUSTUP_HOME
RUN chmod -R a+rw $CARGO_HOME

COPY entrypoint.sh /entrypoint.sh

# Create non-root user
RUN adduser --disabled-password --gecos "" sleeper \
    && usermod -aG sudo,docker,rust sleeper \
    && passwd -d sleeper
USER sleeper

# Add Rust toolchain to path for non-interactive shells, as by default it is only added to .bashrc
ENV PATH=$CARGO_HOME/bin:$PATH

# Set variable telling cross-rs Rust cross compiler that it is already inside a container
ENV CROSS_CONTAINER_IN_CONTAINER=true

# Set command line prompt to show you're in the builder Docker container
RUN echo 'export PS1="\[🐳\] \[\e[0;36m\]\u@sleeper-builder\[\e[0m\]: \w # "' >> ~/.bashrc

WORKDIR /sleeper-builder

ENTRYPOINT [ "/entrypoint.sh" ]
