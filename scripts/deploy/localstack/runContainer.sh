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

docker-compose -f docker-compose.yml up -d 
echo "Running localstack container on port 4566\n"
echo "To use sleeper with this container, set the AWS_ENDPOINT_URL environment variable:"
echo "export AWS_ENDPOINT_URL=http://localhost:4566"
echo ""
echo "To revert to using the default AWS endpoint, you can unset this environment variable:"
echo "unset AWS_ENDPOINT_URL"