#  Copyright 2022-2026 Crown Copyright
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

from sleeper import SleeperClient
from sleeper.properties import CommonCdkProperty, CommonProperty, InstanceProperties
from tests.sleeper.local_aws import LocalAws


class LocalStackSleeperClient:
    @staticmethod
    def create(properties: InstanceProperties) -> SleeperClient:
        return SleeperClient(
            instance_id=properties.get(CommonProperty.ID),
            account_name=properties.get(CommonCdkProperty.ACCOUNT),
            s3_client=LocalAws.s3_client(),
            s3_resource=LocalAws.s3_resource(),
            s3_fs=LocalAws.s3fs(),
            sqs_resource=LocalAws.sqs_resource(),
            dynamo_resource=LocalAws.dynamo_resource(),
        )
