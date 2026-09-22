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
FROM ubuntu:24.04

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Amazon Corretto JDK
RUN curl -fsSL https://apt.corretto.aws/corretto.key | gpg --dearmor --yes -o /etc/apt/keyrings/corretto-keyring.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main"  \
    | tee /etc/apt/sources.list.d/corretto.list > /dev/null
RUN apt-get update && apt-get install -y --no-install-recommends \
    java-17-amazon-corretto-jdk \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for images built from this one to run as. The IDs are set explicitly so they
# don't shift if a later base image adds system users of its own. 28157 chosen randomly bettween 1000 and 32767.
# /mnt/scratch is pre-created and owned by sleeper because ECS ingest tasks use it as a local working
# directory (see ECSIngestTaskRunner).
RUN groupadd --system --gid 28157 sleeper \
    && useradd --system --uid 28157 --gid sleeper --no-create-home sleeper \
    && mkdir -p /mnt/scratch \
    && chown sleeper:sleeper /mnt/scratch

# The sleeper user has no home directory and can't write to /, so point both at /tmp. Otherwise
# anything writing to either fails, which is easy to miss as it's usually not fatal.
ENV HOME=/tmp
WORKDIR /tmp
USER sleeper
