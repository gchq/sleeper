# Copyright 2022 Crown Copyright
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
ARG TARGETARCH
FROM nvcr.io/nvidia/rapidsai/rapidsai-core:22.10-cuda11.5-base-ubuntu20.04-py3.9 as base

# Install some essentials
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y zip unzip tar wget htop nano less xxd hexedit openjdk-8-jre-headless

# Create proc. user
RUN useradd -ms /bin/bash worker
USER worker
WORKDIR /home/worker
# Install some runtime dependencies
RUN /opt/conda/condabin/mamba install -y -c nvidia -c conda-forge -c rapidsai msgpack-python gmp cuda-nvtx cuda-cudart

# Now create builder image with extra tools built in
FROM base AS compiler
ARG TARGETARCH
USER root
RUN apt-get install -y gcc g++ make clang
USER worker
WORKDIR /home/worker
# Install CUDA and compiler and cmake
RUN /opt/conda/condabin/mamba install -y -c nvidia -c conda-forge cmake make cuda-nvcc cuda-cudart-dev libopenssl-static pkg-config

# Copy and build the code
FROM compiler AS builder
USER worker
RUN mkdir -p /home/worker/compact
COPY --chown=worker:worker . /home/worker/compact
WORKDIR /home/worker/compact
RUN bash shell/build.sh

# Now we have a built image, copy it across to the final image
FROM base AS final
COPY --from=builder /home/worker/compact/build /compact
COPY compaction-job-execution.jar /compaction-job-execution.jar
COPY run.sh /run.sh
RUN ["chmod", "+x", "/run.sh"]

ENTRYPOINT ["/run.sh"]
