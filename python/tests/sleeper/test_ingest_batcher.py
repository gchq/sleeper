import json

import pytest
from mypy_boto3_sqs.service_resource import Queue

from sleeper.ingest_batcher import IngestBatcherSender, IngestBatcherSubmitRequest
from sleeper.properties.cdk_defined_properties import IngestCdkProperty
from sleeper.properties.instance_properties import InstanceProperties
from tests.sleeper.localstack import LocalStack
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


def test_ingest_batcher(queue: Queue, sender: IngestBatcherSender):
    # Given
    request = IngestBatcherSubmitRequest(table_name="test-table", files=["file-1.parquet"])

    # When
    sender.send(request)

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


def receive_messages(queue: Queue) -> list[dict]:
    messages = queue.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: json.loads(message.body), messages))
