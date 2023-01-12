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
ARG TARGETARCH
FROM nvcr.io/nvidia/rapidsai/rapidsai-core:22.12-cuda11.5-base-ubuntu20.04-py3.9 as base

# Install some essentials
RUN ["apt-get", "update"]
RUN ["apt-get", "upgrade", "-y"]
RUN ["apt-get", "install", "-y", "htop", "nano", "openjdk-11-jre-headless"]

# Create proc. user
RUN ["useradd", "-ms", "/bin/bash", "worker"]
USER worker
WORKDIR /home/worker
# Install some runtime dependencies
RUN ["/opt/conda/condabin/mamba", "install", "-y", "-c", "nvidia", "-c", "conda-forge", "gmp", "cuda-nvtx=11.5", "cuda-cudart=11.5"]

# Now create builder image with extra tools built in
FROM base AS compiler
ARG TARGETARCH
USER root
RUN ["apt-get", "install", "-y", "gcc", "g++", "make", "clang", "clang-tidy", "clang-format"]
USER worker
WORKDIR /home/worker
# Install CUDA and compiler and cmake (if upgrading to CUDA 12, add cuda-cudart-static as a dependency!)
RUN ["/opt/conda/condabin/mamba", "install", "-y", "-c", "nvidia", "-c", "conda-forge", "cmake", "cmakelang", "cuda-nvcc=11.5", "cuda-cudart-dev=11.5", "libopenssl-static", "pkg-config"]

# Copy and build the code
FROM compiler AS builder
USER worker
RUN ["mkdir", "-p","/home/worker/compact"]
COPY --chown=worker:worker . /home/worker/compact
WORKDIR /home/worker/compact
RUN ["./gpu_build.sh"]

# Now we have a built image, copy it across to the final image
FROM base AS final
USER root
COPY --from=builder /home/worker/compact/build /compact
COPY compaction-job-execution.jar /compaction-job-execution.jar
COPY run.sh /run.sh
RUN ["chmod", "+x", "/run.sh"]

ENTRYPOINT ["/run.sh"]
