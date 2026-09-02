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

ARG BASE_IMAGE=public.ecr.aws/emr-on-eks/spark/emr-7.12.0:latest
FROM ${BASE_IMAGE}

USER root

RUN mkdir -p /opt/spark/workdir && chown hadoop:hadoop /opt/spark/workdir
COPY --chown=hadoop:hadoop ./bulk-import-runner.jar /opt/spark/workdir/

COPY ./sleeper-entrypoint.sh /opt/sleeper-entrypoint.sh
RUN chmod +x /opt/sleeper-entrypoint.sh

USER hadoop:hadoop
ENTRYPOINT ["/opt/sleeper-entrypoint.sh"]