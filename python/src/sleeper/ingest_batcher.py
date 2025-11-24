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

import json

from mypy_boto3_sqs import SQSServiceResource

from sleeper.properties.cdk_defined_properties import IngestCdkProperty
from sleeper.properties.instance_properties import InstanceProperties


class IngestBatcherSubmitRequest:
    def __init__(self, table_name: str = None, files: list[str] = None):
        if table_name is None:
            raise ValueError("table_name must be specified")
        if files is None:
            raise ValueError("files must be specified")
        self.table_name = table_name
        self.files = files

    def to_json(self) -> str:
        obj = {"tableName": self.table_name, "files": self.files}
        return json.dumps(obj)


class IngestBatcherSender:
    def __init__(self, sqs: SQSServiceResource, properties: InstanceProperties):
        self.sqs = sqs
        self.properties = properties

    def send(self, request: IngestBatcherSubmitRequest):
        queue_url = self.properties.get(IngestCdkProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL)
        self.sqs.Queue(queue_url).send_message(MessageBody=request.to_json())
