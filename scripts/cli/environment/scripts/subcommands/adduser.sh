#!/usr/bin/env bash
# Copyright 2022-2024 Crown Copyright
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
unset CDPATH

if [ "$#" -lt 1 ]; then
  echo "Usage: environment adduser <username>"
  exit 1
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
USERNAME="$1"

"$THIS_DIR/setuser.sh"
"$THIS_DIR/connect.sh" \
  cloud-init status --wait \
  "&&" sudo adduser --disabled-password --gecos "-" "$USERNAME" \
  "&&" sudo passwd -d "$USERNAME" \
  "&&" sudo usermod -aG sudo "$USERNAME" \
  "&&" sudo usermod -aG docker "$USERNAME" \
  "&&" sudo runuser --login "$USERNAME" -c '"git clone https://github.com/gchq/sleeper.git ~/.sleeper/builder/sleeper"' \
  "&&" sudo runuser --login "$USERNAME" -c '"~/.sleeper/builder/sleeper/scripts/cli/install.sh"'
"$THIS_DIR/setuser.sh" "$USERNAME"
