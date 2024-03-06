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
ARG MODE=no_tracing

FROM apache/spark:3.4.1-scala2.12-java11-ubuntu as build_no_tracing

FROM build_no_tracing as build_tracing

ONBUILD ADD https://github.com/aws-observability/aws-otel-java-instrumentation/releases/latest/download/aws-opentelemetry-agent.jar /opt/aws-opentelemetry-agent.jar
ONBUILD ENV JAVA_TOOL_OPTIONS=-javaagent:/opt/aws-opentelemetry-agent.jar

FROM build_${MODE}

ENV PATH="$PATH:/opt/spark/bin"
USER root
RUN mkdir /opt/spark/workdir
USER spark
COPY ./bulk-import-runner.jar /opt/spark/workdir
