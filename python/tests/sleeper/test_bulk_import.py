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

import pytest
from mypy_boto3_sqs.service_resource import Queue

from sleeper.client import SleeperClient
from sleeper.properties.cdk_defined_properties import CommonCdkProperty, IngestCdkProperty
from sleeper.properties.config_bucket import save_instance_properties
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.properties.user_defined_properties import CommonProperty
from tests.sleeper.localstack import LocalStack
from tests.sleeper.localstack_sleeper_client import LocalStackSleeperClient
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


# This test will run for each of the values for the pytest fixture platform
def should_put_bulk_export_message_on_the_queue(sleeper_client: SleeperClient, bulk_import_queue: Queue, properties: InstanceProperties, platform: str):
    # Given
    instance_id = properties.get(CommonProperty.ID)

    # When
    sleeper_client.bulk_import_parquet_files_from_s3(table_name="test-table", files=["file1.parquet"], id=instance_id, platform=platform, platform_spec=None, class_name=None)

    # Then
    expected_message_json = [{"id": instance_id, "tableName": "test-table", "files": ["file1.parquet"]}]
    messages = receive_messages(bulk_import_queue)

    assert messages == expected_message_json


def should_put_full_bulk_export_message_on_the_queue_with_empty_platformspec(sleeper_client: SleeperClient, bulk_import_queue: Queue, properties: InstanceProperties, platform: str):
    # Given
    instance_id = properties.get(CommonProperty.ID)
    platformSpec = {}

    # When
    sleeper_client.bulk_import_parquet_files_from_s3(
        table_name="test-table", files=["file1.parquet"], id=instance_id, platform=platform, platform_spec=platformSpec, class_name="SleeperBulkImportTest"
    )

    # Then
    expected_message_json = [{"id": instance_id, "tableName": "test-table", "files": ["file1.parquet"], "platformSpec": platformSpec, "className": "SleeperBulkImportTest"}]
    messages = receive_messages(bulk_import_queue)

    assert messages == expected_message_json


def should_put_full_bulk_export_message_on_the_queue_with_provided_patformspec(sleeper_client: SleeperClient, bulk_import_queue: Queue, properties: InstanceProperties, platform: str):
    # Given
    instance_id = properties.get(CommonProperty.ID)
    platformSpec = {"test": "data"}

    # When
    sleeper_client.bulk_import_parquet_files_from_s3(
        table_name="test-table", files=["file1.parquet"], id=instance_id, platform=platform, platform_spec=platformSpec, class_name="SleeperBulkImportTest"
    )

    # Then
    expected_message_json = [{"id": instance_id, "tableName": "test-table", "files": ["file1.parquet"], "platformSpec": platformSpec, "className": "SleeperBulkImportTest"}]
    messages = receive_messages(bulk_import_queue)

    assert messages == expected_message_json


@pytest.fixture
def sleeper_client(properties: InstanceProperties) -> SleeperClient:
    LocalStack.create_bucket(properties.get(CommonCdkProperty.CONFIG_BUCKET))
    save_instance_properties(LocalStack.s3_resource(), properties)
    return LocalStackSleeperClient.create(properties)


@pytest.fixture
def bulk_import_queue() -> Queue:
    return LocalStack.create_queue()


@pytest.fixture(params=["EMR", "PersistentEMR", "EKS", "EMRServerless"])
def platform(request) -> str:
    return request.param


@pytest.fixture
def properties(bulk_import_queue: Queue, platform: str) -> InstanceProperties:
    properties = create_test_instance_properties()
    if platform == "EMR":
        queue_property = IngestCdkProperty.BULK_IMPORT_EMR_QUEUE_URL
    elif platform == "PersistentEMR":
        queue_property = IngestCdkProperty.BULK_IMPORT_PERSISTENT_EMR_QUEUE_URL
    elif platform == "EKS":
        queue_property = IngestCdkProperty.BULK_IMPORT_EKS_QUEUE_URL
    else:
        queue_property = IngestCdkProperty.BULK_IMPORT_EMR_SERVERLESS_QUEUE_URL
    properties.set(queue_property, bulk_import_queue.url)
    return properties


def receive_messages(queue: Queue):
    messages = queue.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: json.loads(message.body), messages))
