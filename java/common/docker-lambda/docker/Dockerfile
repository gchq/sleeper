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

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    ca-certificates \
    curl \
    gnupg && rm -rf /var/lib/apt/lists/*

# Install Amazon Corretto JDK
RUN curl -fsSL https://apt.corretto.aws/corretto.key | gpg --dearmor --yes -o /etc/apt/keyrings/corretto-keyring.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main"  \
    | tee /etc/apt/sources.list.d/corretto.list > /dev/null
RUN apt-get update && apt-get install -y \
    java-17-amazon-corretto-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install Lambda Runtime Interface Emulator
RUN curl -fsSL -O https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/download/v1.31/aws-lambda-rie && \
    mv ./aws-lambda-rie /usr/local/bin/ && \
    chmod +x /usr/local/bin/aws-lambda-rie

WORKDIR /function
COPY lambda.jar ./
COPY aws-lambda-java-runtime.jar ./
COPY entrypoint.sh /
ENTRYPOINT [ "/entrypoint.sh" ]
