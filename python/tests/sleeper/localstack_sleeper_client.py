#  Copyright 2022-2025 Crown Copyright
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from sleeper.client import SleeperClient
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.properties.user_defined_properties import CommonProperty
from tests.sleeper.localstack import LocalStack


class LocalStackSleeperClient:
    @staticmethod
    def create(properties: InstanceProperties) -> SleeperClient:
        return SleeperClient(
            instance_id=properties.get(CommonProperty.ID),
            s3_client=LocalStack.s3_client(),
            s3_resource=LocalStack.s3_resource(),
            s3_fs=LocalStack.s3fs(),
            sqs_resource=LocalStack.sqs_resource(),
            dynamo_resource=LocalStack.dynamo_resource(),
        )
