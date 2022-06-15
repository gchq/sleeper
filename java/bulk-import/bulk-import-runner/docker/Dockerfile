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
ARG BUILDER_IMAGE_NAME=maven
ARG BUILDER_IMAGE_TAG=3.8-openjdk-8-slim

ARG BASE_IMAGE_NAME=openjdk
ARG BASE_IMAGE_TAG=8-jre-slim

ARG SPARK_VERSION=3.1.2
ARG HADOOP_VERSION=3.2.1

FROM ${BUILDER_IMAGE_NAME}:${BUILDER_IMAGE_TAG} as builder

ARG SPARK_VERSION
ARG HADOOP_VERSION

ARG SPARK_DOWNLOAD_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz

# Build Native Hadoop Libraries
WORKDIR /workdir
COPY ./build/build-hadoop.sh .
RUN ./build-hadoop.sh

# This is done in a two step process to avoid unnecessary hadoop builds when editing the extraction script

COPY ./build/extract-native-libs.sh .
RUN ./extract-native-libs.sh

# Download Spark
RUN curl -s ${SPARK_DOWNLOAD_URL} | tar -C /opt -xz

# Slim down spark
RUN echo "Before slimming: $(du -sh /opt/spark-${SPARK_VERSION}-bin-hadoop3.2)" && \
    rm -r /opt/spark*/examples && \
    rm -r /opt/spark*/yarn && \
    cp /opt/spark*/kubernetes/dockerfiles/spark/entrypoint.sh /opt && rm -r /opt/spark*/kubernetes && \
    rm -r /opt/spark*/python && \
    rm -r /opt/spark*/R && \
    rm -r /opt/spark*/data && \
    echo "After slimming: $(du -sh /opt/spark-${SPARK_VERSION}-bin-hadoop3.2)"

# Add workdir
RUN mkdir /opt/spark-${SPARK_VERSION}-bin-hadoop3.2/workdir

FROM ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}

ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG USER=spark
ARG GROUP=spark

RUN apt-get -qq update && \
    apt-get -y -qq install tini libzstd1

RUN groupadd ${GROUP} && useradd --home-dir /opt/spark --gid ${GROUP} --no-create-home --shell /bin/bash ${USER}

COPY --from=builder --chown=${USER}:${GROUP} /opt/spark-${SPARK_VERSION}-bin-hadoop3.2 /opt/spark-${SPARK_VERSION}-bin-hadoop3.2
COPY --from=builder --chown=${USER}:${GROUP} /opt/entrypoint.sh /opt/
COPY --from=builder --chown=${USER}:${GROUP} /opt/hadoop-${HADOOP_VERSION} /opt/hadoop-${HADOOP_VERSION}

RUN cd /opt && \
    ln -s ./spark-${SPARK_VERSION}-bin-hadoop3.2 ./spark && \
    ln -s ./hadoop-${HADOOP_VERSION} ./hadoop

ENV SPARK_HOME=/opt/spark
ENV LD_LIBRARY_PATH=/opt/hadoop/lib/native

ENV PATH="$PATH:${SPARK_HOME}/bin"
USER ${USER}

WORKDIR /opt/spark/workdir
RUN mv ../conf/log4j.properties.template ./log4j.properties
COPY ./bulk-import-runner.jar /opt/spark/workdir
ENTRYPOINT ["/opt/entrypoint.sh"]
