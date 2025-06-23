import json

import pytest
from mypy_boto3_sqs.service_resource import Queue

from sleeper.client import SleeperClient
from sleeper.ingest_batcher import IngestBatcherSender, IngestBatcherSubmitRequest
from sleeper.properties.cdk_defined_properties import CommonCdkProperty, IngestCdkProperty
from sleeper.properties.config_bucket import save_instance_properties
from sleeper.properties.instance_properties import InstanceProperties
from tests.sleeper.localstack import LocalStack
from tests.sleeper.localstack_sleeper_client import LocalStackSleeperClient
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


def test_send_to_ingest_batcher(queue: Queue, sender: IngestBatcherSender):
    # Given
    request = IngestBatcherSubmitRequest(table_name="test-table", files=["file-1.parquet"])

    # When
    sender.send(request)

    # Then
    assert [{"tableName": "test-table", "files": ["file-1.parquet"]}] == receive_messages(queue)


def test_send_to_ingest_batcher_with_client(queue: Queue, sleeper_client: SleeperClient):
    # When
    sleeper_client.submit_to_ingest_batcher("test-table", ["file-1.parquet"])

    # Then
    assert [{"tableName": "test-table", "files": ["file-1.parquet"]}] == receive_messages(queue)


@pytest.fixture
def queue() -> Queue:
    return LocalStack.create_queue()


@pytest.fixture
def properties(queue: Queue) -> InstanceProperties:
    properties = create_test_instance_properties()
    properties.set(IngestCdkProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL, queue.url)
    return properties


@pytest.fixture
def sender(properties: InstanceProperties) -> IngestBatcherSender:
    return IngestBatcherSender(LocalStack.sqs_resource(), properties)


@pytest.fixture
def sleeper_client(properties: InstanceProperties) -> SleeperClient:
    LocalStack.create_bucket(properties.get(CommonCdkProperty.CONFIG_BUCKET))
    save_instance_properties(LocalStack.s3_resource(), properties)
    return LocalStackSleeperClient.create(properties)


def receive_messages(queue: Queue) -> list[dict]:
    messages = queue.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: json.loads(message.body), messages))
