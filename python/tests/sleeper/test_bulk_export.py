import json
import uuid

import pytest
from mypy_boto3_sqs.service_resource import Queue

from sleeper.bulk_export import BulkExportQuery, BulkExportSender
from sleeper.properties.cdk_defined_properties import CommonCdkProperty, QueryCdkProperty
from sleeper.properties.config_bucket import save_instance_properties
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.sleeper import SleeperClient
from tests.sleeper.instance_properties_helper import create_test_instance_properties
from tests.sleeper.localstack import LocalStack
from tests.sleeper.localstack_sleeper_client import LocalStackSleeperClient


def test_bulk_export(sender: BulkExportSender, queue: Queue):
    # Given
    query = BulkExportQuery(export_id="test-export", table_name="test-table")

    # When
    sender.send(query)

    # Then
    assert [{"exportId": "test-export", "tableName": "test-table"}] == receive_messages(queue)


def test_bulk_export_with_client(sleeper_client: SleeperClient, queue: Queue):
    # Given
    query = BulkExportQuery(export_id="test-export", table_name="test-table")

    # When
    sleeper_client.bulk_export(query)

    # Then
    assert [{"exportId": "test-export", "tableName": "test-table"}] == receive_messages(queue)


@pytest.fixture
def sleeper_client(properties: InstanceProperties) -> SleeperClient:
    LocalStack.create_bucket(properties.get(CommonCdkProperty.CONFIG_BUCKET))
    save_instance_properties(LocalStack.s3_resource(), properties)
    return LocalStackSleeperClient.create(properties)


@pytest.fixture
def properties(queue: Queue) -> InstanceProperties:
    properties = create_test_instance_properties()
    properties.set(QueryCdkProperty.BULK_EXPORT_QUEUE, queue.url)
    return properties


@pytest.fixture
def sender(properties: InstanceProperties) -> BulkExportSender:
    return BulkExportSender(LocalStack.sqs_resource(), properties)


@pytest.fixture
def queue() -> Queue:
    return LocalStack.create_queue()


def receive_messages(queue: Queue):
    messages = queue.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: json.loads(message.body), messages))
