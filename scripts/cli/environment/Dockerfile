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
    ssh \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install Session Manager plugin
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "$dpkgArch" in \
    arm64) ssmArch='ubuntu_arm64' ;; \
    *) ssmArch='ubuntu_64bit' ;; \
    esac; \
    curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/$ssmArch/session-manager-plugin.deb" -o "session-manager-plugin.deb"
RUN dpkg -i session-manager-plugin.deb

# Set command line prompt to show you're in the environment Docker container
RUN echo 'export PS1="\[🐳\] \[\e[0;36m\]\u@sleeper-environment\[\e[0m\]: \w # "' >> ~/.bashrc

WORKDIR /sleeper
COPY . .

ENV PATH=/sleeper/bin:$PATH

CMD [ "bash" ]
