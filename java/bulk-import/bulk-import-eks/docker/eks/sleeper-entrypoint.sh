#!/usr/bin/env bash
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

set -euo pipefail

if [ -n "${EXECUTOR_POD_TEMPLATE:-}" ]; then
    printf '%s' "$EXECUTOR_POD_TEMPLATE" > /tmp/executor-template.yaml

    if [ -n "${SPARK_APPLICATION_ID:-}" ]; then
        sed -i "s/spark-app-selector-placeholder/${SPARK_APPLICATION_ID}/g" /tmp/executor-template.yaml
    fi
fi

exec /opt/entrypoint.sh "$@"
