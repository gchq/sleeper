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
ARG BASE_IMAGE=ghcr.io/gchq/sleeper-java-base:0.35.4-SNAPSHOT
FROM ${BASE_IMAGE}

# Install Lambda Runtime Interface Emulator
RUN curl -fsSL -O https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/download/v1.35/aws-lambda-rie && \
    mv ./aws-lambda-rie /usr/local/bin/ && \
    chmod +x /usr/local/bin/aws-lambda-rie

WORKDIR /function
COPY lambda.jar ./
COPY aws-lambda-java-runtime.jar ./
COPY entrypoint.sh /
ENTRYPOINT [ "/entrypoint.sh" ]
