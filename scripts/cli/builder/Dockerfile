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
    && rm -rf /var/lib/apt/lists/*

# Set command line prompt to show you're in the builder Docker container
RUN echo 'export PS1="\[🐳\] \[\e[0;36m\]\u@sleeper-builder\[\e[0m\]: \w # "' >> ~/.bashrc

WORKDIR /sleeper-builder

CMD [ "bash" ]
