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

FROM apache/spark:3.5.5-scala2.12-java17-ubuntu

ENV PATH="$PATH:/opt/spark/bin"
USER root
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
RUN rm /opt/spark/jars/*
RUN mkdir /opt/spark/workdir
USER spark

# Replace Spark jars with versions managed by Sleeper (the version of Spark must match)
COPY ./spark/* /opt/spark/jars
COPY ./bulk-import-runner.jar /opt/spark/workdir
