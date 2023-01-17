#!/usr/bin/env bash

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

set -e

if [ "$#" -lt 1 ]; then
  GIT_REF="main"
  REMOTE_TAG="latest"
else
  GIT_REF="$1"
  REMOTE_TAG="$1"
fi

REMOTE_IMAGE="ghcr.io/gchq/sleeper-local:$REMOTE_TAG"
LOCAL_IMAGE="sleeper-local:current"

docker pull "$REMOTE_IMAGE"
docker tag "$REMOTE_IMAGE" "$LOCAL_IMAGE"

echo "Installing Sleeper to system path (may require elevated permissions)"
sudo curl "https://raw.githubusercontent.com/gchq/sleeper/$GIT_REF/scripts/local/runInDocker.sh" --output /usr/local/bin/sleeper
sudo chmod a+x /usr/local/bin/sleeper
