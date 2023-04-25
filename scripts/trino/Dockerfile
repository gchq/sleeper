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

FROM trinodb/trino:390

ENV PLUGINS_DIR=/usr/lib/trino/plugin
ENV ALL_PLUGINS_DIR=/usr/lib/trino/plugin.all

RUN mv $PLUGINS_DIR $ALL_PLUGINS_DIR # Back up all of the existing plugins
RUN mkdir -p $PLUGINS_DIR
RUN cp -r $ALL_PLUGINS_DIR/tpch $PLUGINS_DIR
RUN cp -r $ALL_PLUGINS_DIR/memory $PLUGINS_DIR
ADD ./jars $PLUGINS_DIR/sleeper
