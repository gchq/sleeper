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

ARG RUN_IMAGE
FROM ${RUN_IMAGE}

ARG SET_UID
ARG SET_GID
ARG SET_DOCKER_GID

# Propagate user IDs to image, to avoid mixed file ownership
USER root
RUN chown ${SET_UID}:${SET_GID} -R /home/sleeper
RUN groupmod -g ${SET_GID} sleeper
RUN groupmod -g ${SET_DOCKER_GID} docker
RUN usermod -u ${SET_UID} -g ${SET_GID} sleeper
USER sleeper
